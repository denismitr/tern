package mysql

import (
	"database/sql"
	"github.com/denismitr/tern/v3/internal/database/sqlgateway"
)

var _ sqlgateway.CtxExecutor = (*sql.Conn)(nil)
//var _ sqlgateway.CtxExecutor = (*sqlgateway.MockctxExecutor)(nil)

//func TestMySQLLocker_Lock(t *testing.T) {
//	t.Parallel()
//
//	t.Run("lock", func(t *testing.T) {
//		lockKey := "foo"
//		lockFor := 5
//		noLock := false
//
//		ctrl := gomock.NewController(t)
//		executor := sqlgateway.NewMockctxExecutor(ctrl)
//
//		ctx := context.Background()
//
//		executor.
//			EXPECT().
//			ExecContext(gomock.Any(), "SELECT GET_LOCK(?, ?)", lockKey, lockFor).
//			Return(nil, nil).
//			Times(1)
//
//		locker := newMySQLLocker(lockKey, lockFor, noLock)
//
//		err := locker.lock(ctx, executor)
//		if err != nil {
//			t.Fatalf("unexpected error %+v", err)
//		}
//	})
//
//	t.Run("no lock", func(t *testing.T) {
//		lockKey := "foo"
//		lockFor := 5
//		noLock := true
//
//		ctrl := gomock.NewController(t)
//		executor := sqlgateway.NewMockctxExecutor(ctrl)
//
//		ctx := context.Background()
//
//		executor.
//			EXPECT().
//			ExecContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
//			Times(0)
//
//		locker := newMySQLLocker(lockKey, lockFor, noLock)
//
//		err := locker.lock(ctx, executor)
//		if err != nil {
//			t.Fatalf("unexpected error %+v", err)
//		}
//	})
//}
//
//func TestMySQLLocker_Unlock(t *testing.T) {
//	t.Parallel()
//
//	t.Run("unlock", func(t *testing.T) {
//		lockKey := "foo"
//		lockFor := 5
//		noLock := false
//
//		ctrl := gomock.NewController(t)
//		executor := sqlgateway.NewMockctxExecutor(ctrl)
//
//		ctx := context.Background()
//
//		executor.
//			EXPECT().
//			ExecContext(gomock.Any(), "SELECT RELEASE_LOCK(?)", lockKey).
//			Return(nil, nil).
//			Times(1)
//
//		locker := newMySQLLocker(lockKey, lockFor, noLock)
//
//		err := locker.unlock(ctx, executor)
//		if err != nil {
//			t.Fatalf("unexpected error %+v", err)
//		}
//	})
//
//	t.Run("no lock", func(t *testing.T) {
//		lockKey := "foo"
//		lockFor := 5
//		noLock := true
//
//		ctrl := gomock.NewController(t)
//		executor := sqlgateway.NewMockctxExecutor(ctrl)
//
//		ctx := context.Background()
//
//		executor.
//			EXPECT().
//			ExecContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
//			Times(0)
//
//		locker := newMySQLLocker(lockKey, lockFor, noLock)
//
//		err := locker.unlock(ctx, executor)
//		if err != nil {
//			t.Fatalf("unexpected error %+v", err)
//		}
//	})
//}
//
//
