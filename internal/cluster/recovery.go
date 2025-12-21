package cluster

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/aritrod3/kv-store/internal/storage"
)

type RecoveryManager struct{}

func (r *RecoveryManager) CatchUpReplica(ctx context.Context, snapshotSrc string, walSrc string, target *storage.Engine) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if target == nil {
		return errors.New("target required")
	}
	if err := copyFile(snapshotSrc, target.SnapshotPath()); err != nil {
		return err
	}
	if err := copyFile(walSrc, target.WALPath()); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return target.RestoreFrom(target.SnapshotPath(), target.WALPath())
}

func copyFile(src string, dst string) error {
	if src == dst {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp, err := os.CreateTemp(filepath.Dir(dst), "copy-*.tmp")
	if err != nil {
		return err
	}
	if _, err := tmp.ReadFrom(in); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), dst)
}
