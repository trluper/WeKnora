package datahub

import (
	"context"
	"errors"
	"testing"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

func recomputeModule(store TestDataStore) *Module {
	return newModule(enabledSettings(), Parts{
		Objects:  &fakeObjectStore{},
		Store:    &fakeUploadStore{},
		TestData: store,
	})
}

func recomputeTask() *asynq.Task {
	return asynq.NewTask(taskTypeRecomputeSummaries, nil)
}

func TestSummaryRecomputeRunsTheBackstop(t *testing.T) {
	store := newFakeTestDataStore()
	store.recomputeResult = 2
	module := recomputeModule(store)

	err := module.handleRecomputeTestSummaries(context.Background(), recomputeTask())

	require.NoError(t, err)
	require.Equal(t, 1, store.recomputeCalls)
}

func TestSummaryRecomputeSucceedsWhenNothingDrifted(t *testing.T) {
	store := newFakeTestDataStore()
	module := recomputeModule(store)

	err := module.handleRecomputeTestSummaries(context.Background(), recomputeTask())

	require.NoError(t, err, "a clean run is not an error")
}

func TestSummaryRecomputeReturnsFailuresForRetry(t *testing.T) {
	store := newFakeTestDataStore()
	store.recomputeErr = errors.New("postgres is down")
	module := recomputeModule(store)

	err := module.handleRecomputeTestSummaries(context.Background(), recomputeTask())

	require.Error(t, err, "a failing recompute must be retried by asynq")
}

func TestSummaryRecomputeFailsLoudlyWithoutAStore(t *testing.T) {
	module := newModule(enabledSettings(), Parts{Objects: &fakeObjectStore{}})

	err := module.handleRecomputeTestSummaries(context.Background(), recomputeTask())

	require.Error(t, err)
}
