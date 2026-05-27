package exex

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"time"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// RPCClient is the subset of a go-ethereum RPC client needed by RPCSource.
type RPCClient interface {
	HeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error)
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
	FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error)
}

// HeadSubscriber is implemented by clients that support new-head subscriptions.
type HeadSubscriber interface {
	SubscribeNewHead(ctx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error)
}

type rpcStartMode uint8

const (
	rpcStartLatest rpcStartMode = iota
	rpcStartBlock
)

// RPCStart configures where an RPCSource starts when the store is empty.
type RPCStart struct {
	mode  rpcStartMode
	block uint64
}

// StartAtLatest starts from the current RPC head without backfilling existing
// blocks. This is the zero-value start mode.
func StartAtLatest() RPCStart {
	return RPCStart{}
}

// StartAtBlock starts by processing block number and every later block.
func StartAtBlock(number uint64) RPCStart {
	return RPCStart{mode: rpcStartBlock, block: number}
}

func (s RPCStart) blockNumber() (uint64, bool) {
	return s.block, s.mode == rpcStartBlock
}

// RPCSourceConfig configures an RPC-backed source.
type RPCSourceConfig struct {
	Filter       ethereum.FilterQuery
	Start        RPCStart
	Store        ChainStore
	HeadBuffer   int
	PollInterval time.Duration
}

// RPCSource drives a Handler from an Ethereum JSON-RPC endpoint.
type RPCSource struct {
	client RPCClient
	config RPCSourceConfig
}

// NewRPCSource creates an RPC-backed source.
func NewRPCSource(client RPCClient, config RPCSourceConfig) (*RPCSource, error) {
	if client == nil {
		return nil, errors.New("exex: nil rpc client")
	}
	if config.Store == nil {
		return nil, errors.New("exex: rpc source config store is required")
	}
	if config.Filter.BlockHash != nil {
		return nil, errors.New("exex: rpc source config filter block hash must be nil")
	}
	if config.Filter.FromBlock != nil || config.Filter.ToBlock != nil {
		return nil, errors.New("exex: use rpc source config start instead of filter from block or to block")
	}
	if config.HeadBuffer <= 0 {
		config.HeadBuffer = 64
	}

	return &RPCSource{client: client, config: config}, nil
}

// Run keeps the source synchronized and applies chain updates to handler.
func (s *RPCSource) Run(ctx context.Context, handler Handler) error {
	if handler == nil {
		return errors.New("exex: nil handler")
	}
	if s.config.PollInterval > 0 {
		return s.runPolling(ctx, handler, s.config.PollInterval)
	}

	subscriber, ok := s.client.(HeadSubscriber)
	if !ok {
		return errors.New("exex: rpc client does not support new-head subscriptions")
	}

	heads := make(chan *types.Header, s.config.HeadBuffer)
	subscription, err := subscriber.SubscribeNewHead(ctx, heads)
	if err != nil {
		return fmt.Errorf("subscribe new heads: %w", err)
	}
	defer subscription.Unsubscribe()

	if err := s.Sync(ctx, handler); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-subscription.Err():
			if err == nil {
				return nil
			}
			return fmt.Errorf("new-head subscription: %w", err)
		case header := <-heads:
			if header == nil {
				continue
			}
			if err := s.processHead(ctx, header, handler); err != nil {
				return err
			}
		}
	}
}

func (s *RPCSource) runPolling(ctx context.Context, handler Handler, interval time.Duration) error {
	if handler == nil {
		return errors.New("exex: nil handler")
	}
	if interval <= 0 {
		return errors.New("exex: poll interval must be positive")
	}

	if err := s.Sync(ctx, handler); err != nil {
		return err
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.Sync(ctx, handler); err != nil {
				return err
			}
		}
	}
}

// Sync processes all canonical blocks between the store head and the current RPC head.
func (s *RPCSource) Sync(ctx context.Context, handler Handler) error {
	if handler == nil {
		return errors.New("exex: nil handler")
	}

	latest, err := s.client.HeaderByNumber(ctx, nil)
	if err != nil {
		return fmt.Errorf("fetch latest header: %w", err)
	}
	if latest == nil || latest.Number == nil {
		return errors.New("exex: latest header is missing a number")
	}

	if err := s.seed(ctx, latest); err != nil {
		return err
	}

	latestNumber := latest.Number.Uint64()
	from, explicitStart := s.config.Start.blockNumber()
	head, ok, err := s.config.Store.Head(ctx)
	if err != nil {
		return err
	}
	if ok {
		if head.Number == math.MaxUint64 {
			return nil
		}
		from = head.Number + 1
	} else if !explicitStart {
		return nil
	}

	if from > latestNumber {
		if ok && head.Hash != latest.Hash() {
			return s.processHead(ctx, latest, handler)
		}
		return nil
	}

	for number := from; number <= latestNumber; number++ {
		header, err := s.client.HeaderByNumber(ctx, uint64ToBig(number))
		if err != nil {
			return fmt.Errorf("fetch header %d: %w", number, err)
		}
		if header == nil {
			return fmt.Errorf("fetch header %d: not found", number)
		}
		if err := s.processHead(ctx, header, handler); err != nil {
			return err
		}
		if number == math.MaxUint64 {
			break
		}
	}

	return nil
}

func (s *RPCSource) processHead(ctx context.Context, header *types.Header, handler Handler) error {
	if handler == nil {
		return errors.New("exex: nil handler")
	}
	if header == nil {
		return errors.New("exex: nil header")
	}
	if header.Number == nil {
		return errors.New("exex: header is missing a number")
	}

	current, ok, err := s.config.Store.Head(ctx)
	if err != nil {
		return err
	}
	if ok && current.Hash == header.Hash() {
		return nil
	}

	if !ok {
		block, err := s.fetchBlock(ctx, header)
		if err != nil {
			return err
		}
		return s.handleUpdate(ctx, handler, nil, []StoredBlock{block})
	}

	headerNumber := header.Number.Uint64()
	if headerNumber == current.Number+1 && header.ParentHash == current.Hash {
		block, err := s.fetchBlock(ctx, header)
		if err != nil {
			return err
		}
		return s.handleUpdate(ctx, handler, nil, []StoredBlock{block})
	}

	reverted, committedHeaders, err := s.reconcile(ctx, current, header)
	if err != nil {
		return err
	}

	committed := make([]StoredBlock, 0, len(committedHeaders))
	for _, committedHeader := range committedHeaders {
		block, err := s.fetchBlock(ctx, committedHeader)
		if err != nil {
			return err
		}
		committed = append(committed, block)
	}

	return s.handleUpdate(ctx, handler, reverted, committed)
}

func (s *RPCSource) seed(ctx context.Context, latest *types.Header) error {
	_, ok, err := s.config.Store.Head(ctx)
	if err != nil || ok {
		return err
	}

	startBlock, explicitStart := s.config.Start.blockNumber()
	if !explicitStart {
		return s.config.Store.UpdateCanonicalChain(ctx, nil, []StoredBlock{headerToStoredBlock(latest, nil)})
	}
	if startBlock == 0 {
		return nil
	}

	anchorNumber := startBlock - 1
	header, err := s.client.HeaderByNumber(ctx, uint64ToBig(anchorNumber))
	if err != nil {
		return fmt.Errorf("fetch start anchor header %d: %w", anchorNumber, err)
	}
	if header == nil {
		return fmt.Errorf("fetch start anchor header %d: not found", anchorNumber)
	}

	block := headerToStoredBlock(header, nil)
	return s.config.Store.UpdateCanonicalChain(ctx, nil, []StoredBlock{block})
}

func (s *RPCSource) reconcile(ctx context.Context, current StoredBlock, header *types.Header) ([]StoredBlock, []*types.Header, error) {
	oldCursor := current
	newCursor := header
	oldReversed := make([]StoredBlock, 0)
	newReversed := make([]*types.Header, 0)

	for oldCursor.Number > newCursor.Number.Uint64() {
		oldReversed = append(oldReversed, oldCursor)
		parent, ok, err := s.config.Store.BlockByHash(ctx, oldCursor.ParentHash)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			return nil, nil, fmt.Errorf("find old ancestor for block %s: parent %s is missing", oldCursor.Hash, oldCursor.ParentHash)
		}
		oldCursor = parent
	}

	for newCursor.Number.Uint64() > oldCursor.Number {
		newReversed = append(newReversed, newCursor)
		parent, err := s.parentHeader(ctx, newCursor)
		if err != nil {
			return nil, nil, err
		}
		newCursor = parent
	}

	for oldCursor.Hash != newCursor.Hash() {
		oldReversed = append(oldReversed, oldCursor)
		parentBlock, ok, err := s.config.Store.BlockByHash(ctx, oldCursor.ParentHash)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			return nil, nil, fmt.Errorf("find old ancestor for block %s: parent %s is missing", oldCursor.Hash, oldCursor.ParentHash)
		}
		oldCursor = parentBlock

		newReversed = append(newReversed, newCursor)
		parentHeader, err := s.parentHeader(ctx, newCursor)
		if err != nil {
			return nil, nil, err
		}
		newCursor = parentHeader
	}

	reverseStoredBlocks(oldReversed)
	reverseHeaders(newReversed)
	return oldReversed, newReversed, nil
}

func (s *RPCSource) parentHeader(ctx context.Context, header *types.Header) (*types.Header, error) {
	if header.Number.Uint64() == 0 {
		return nil, fmt.Errorf("block %s has no parent", header.Hash())
	}

	parent, err := s.client.HeaderByHash(ctx, header.ParentHash)
	if err != nil {
		return nil, fmt.Errorf("fetch parent header %s: %w", header.ParentHash, err)
	}
	if parent == nil {
		return nil, fmt.Errorf("fetch parent header %s: not found", header.ParentHash)
	}
	return parent, nil
}

func (s *RPCSource) fetchBlock(ctx context.Context, header *types.Header) (StoredBlock, error) {
	if header.Number == nil {
		return StoredBlock{}, errors.New("exex: header is missing a number")
	}

	hash := header.Hash()
	query := s.config.Filter
	query.BlockHash = &hash
	query.FromBlock = nil
	query.ToBlock = nil

	logs, err := s.client.FilterLogs(ctx, query)
	if err != nil {
		return StoredBlock{}, fmt.Errorf("fetch logs for block %d %s: %w", header.Number.Uint64(), hash, err)
	}

	return headerToStoredBlock(header, logs), nil
}

func (s *RPCSource) handleUpdate(ctx context.Context, handler Handler, reverted []StoredBlock, committed []StoredBlock) error {
	if len(reverted) == 0 && len(committed) == 0 {
		return nil
	}

	var notification Notification
	switch {
	case len(reverted) == 0:
		notification = NewChainCommitted(chainFromStoredBlocks(committed))
	case len(committed) == 0:
		notification = NewChainReverted(chainFromStoredBlocks(reverted))
	default:
		notification = NewChainReorged(chainFromStoredBlocks(reverted), chainFromStoredBlocks(committed))
	}

	if err := notification.Apply(ctx, handler); err != nil {
		return err
	}

	return s.config.Store.UpdateCanonicalChain(ctx, reverted, committed)
}

func headerToStoredBlock(header *types.Header, logs []types.Log) StoredBlock {
	return StoredBlock{
		Number:     header.Number.Uint64(),
		Hash:       header.Hash(),
		ParentHash: header.ParentHash,
		Logs:       cloneLogs(logs),
	}
}

func chainFromStoredBlocks(blocks []StoredBlock) Chain {
	if len(blocks) == 0 {
		return Chain{FromBlock: 1, ToBlock: 0}
	}

	chain := Chain{
		FromBlock: blocks[0].Number,
		ToBlock:   blocks[len(blocks)-1].Number,
	}
	for _, block := range blocks {
		if len(block.Logs) == 0 {
			continue
		}
		chain.Blocks = append(chain.Blocks, BlockLogs{
			Number: block.Number,
			Hash:   block.Hash,
			Logs:   cloneLogs(block.Logs),
		})
	}
	return chain
}

func reverseStoredBlocks(blocks []StoredBlock) {
	for i, j := 0, len(blocks)-1; i < j; i, j = i+1, j-1 {
		blocks[i], blocks[j] = blocks[j], blocks[i]
	}
}

func reverseHeaders(headers []*types.Header) {
	for i, j := 0, len(headers)-1; i < j; i, j = i+1, j-1 {
		headers[i], headers[j] = headers[j], headers[i]
	}
}

func uint64ToBig(number uint64) *big.Int {
	return new(big.Int).SetUint64(number)
}
