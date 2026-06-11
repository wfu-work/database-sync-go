package services

import (
	"testing"

	"database-sync-go/domains"
	"database-sync-go/utils"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestDataSourceSaveEncryptsAndMasksPassword(t *testing.T) {
	db := newDataSourceTestDB(t)
	service := ServiceGroupApp.DataSourceService.WithDB(db)

	saved, err := service.Save(SaveDataSourceRequest{
		Name:     "mysql",
		Type:     domains.DataSourceTypeMySQL,
		Host:     "127.0.0.1",
		Username: "root",
		Password: "plain-password",
		Database: "demo",
	})
	if err != nil {
		t.Fatalf("save datasource failed: %v", err)
	}
	if saved.Password != "******" {
		t.Fatalf("public datasource password should be masked: %q", saved.Password)
	}

	var stored domains.DataSource
	if err := db.Where("guid = ?", saved.Guid).First(&stored).Error; err != nil {
		t.Fatalf("query stored datasource failed: %v", err)
	}
	if !utils.IsEncryptedSecret(stored.Password) {
		t.Fatalf("stored password should be encrypted: %q", stored.Password)
	}
	plain, err := utils.DecryptSecret(stored.Password)
	if err != nil {
		t.Fatalf("decrypt stored password failed: %v", err)
	}
	if plain != "plain-password" {
		t.Fatalf("unexpected stored password: %q", plain)
	}
}

func TestDataSourceSaveKeepsMaskedPasswordOnUpdate(t *testing.T) {
	db := newDataSourceTestDB(t)
	service := ServiceGroupApp.DataSourceService.WithDB(db)

	saved, err := service.Save(SaveDataSourceRequest{
		Name:     "mysql",
		Type:     domains.DataSourceTypeMySQL,
		Host:     "127.0.0.1",
		Username: "root",
		Password: "initial-password",
		Database: "demo",
	})
	if err != nil {
		t.Fatalf("save datasource failed: %v", err)
	}
	var before domains.DataSource
	if err := db.Where("guid = ?", saved.Guid).First(&before).Error; err != nil {
		t.Fatalf("query datasource failed: %v", err)
	}

	updated, err := service.Save(SaveDataSourceRequest{
		Guid:     saved.Guid,
		Name:     "mysql updated",
		Type:     domains.DataSourceTypeMySQL,
		Host:     "127.0.0.1",
		Username: "root",
		Password: "******",
		Database: "demo",
	})
	if err != nil {
		t.Fatalf("update datasource failed: %v", err)
	}
	if updated.Password != "******" {
		t.Fatalf("updated public password should be masked: %q", updated.Password)
	}

	var after domains.DataSource
	if err := db.Where("guid = ?", saved.Guid).First(&after).Error; err != nil {
		t.Fatalf("query updated datasource failed: %v", err)
	}
	if after.Password != before.Password {
		t.Fatal("masked update should keep existing encrypted password")
	}
}

func TestMigratePlaintextPasswords(t *testing.T) {
	db := newDataSourceTestDB(t)
	service := ServiceGroupApp.DataSourceService.WithDB(db)
	row := domains.DataSource{
		Name:     "legacy",
		Type:     domains.DataSourceTypeMySQL,
		Host:     "127.0.0.1",
		Port:     3306,
		Username: "root",
		Password: "legacy-password",
		Database: "demo",
		Status:   int(domains.StatusEnabled),
	}
	if err := db.Create(&row).Error; err != nil {
		t.Fatalf("create legacy datasource failed: %v", err)
	}
	if err := service.MigratePlaintextPasswords(); err != nil {
		t.Fatalf("migrate plaintext password failed: %v", err)
	}
	var migrated domains.DataSource
	if err := db.Where("guid = ?", row.Guid).First(&migrated).Error; err != nil {
		t.Fatalf("query migrated datasource failed: %v", err)
	}
	if !utils.IsEncryptedSecret(migrated.Password) {
		t.Fatalf("migrated password should be encrypted: %q", migrated.Password)
	}
	plain, err := utils.DecryptSecret(migrated.Password)
	if err != nil {
		t.Fatalf("decrypt migrated password failed: %v", err)
	}
	if plain != "legacy-password" {
		t.Fatalf("unexpected migrated password: %q", plain)
	}
}

func newDataSourceTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	if err := db.AutoMigrate(&domains.DataSource{}, &domains.SyncTask{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	return db
}
