package main

import "testing"

func TestMigrateURLUsesCockroachDriver(t *testing.T) {
	for in, want := range map[string]string{
		"postgresql://u:p%40ss@crdb:26257/aicontrolplane?sslmode=require": "cockroachdb://u:p%40ss@crdb:26257/aicontrolplane?sslmode=require",
		"cockroachdb://u@crdb:26257/aicontrolplane-staging":               "cockroachdb://u@crdb:26257/aicontrolplane-staging",
	} {
		got, err := migrateURL(in)
		if err != nil || got != want {
			t.Errorf("migrateURL(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := migrateURL("mysql://u@h/db"); err == nil {
		t.Error("mysql:// accepted; want an error")
	}
}

func TestDatabaseNameHidesCredentials(t *testing.T) {
	if got := databaseName("cockroachdb://u:secret@crdb:26257/aicontrolplane-staging?sslmode=require"); got != "aicontrolplane-staging" {
		t.Errorf("got %q", got)
	}
}
