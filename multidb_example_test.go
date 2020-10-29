package multidb

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/moapis/multidb/drivers/postgresql"
)

func ExampleMultiDB() {
	const connStr = "user=pqgotest dbname=pqgotest host=%s.moapis.org"

	hosts := []string{
		"db1",
		"db2",
		"db3",
	}

	mdb := &MultiDB{
		MasterFunc: IsMaster(postgresql.MasterQuery),
	}
	defer mdb.Close()

	for _, host := range hosts {
		db, err := sql.Open("postgres", fmt.Sprintf(connStr, host))
		if err != nil {
			panic(err)
		}

		mdb.Add(host, db)
	}
}

var (
	mdb *MultiDB
)

func ExampleMultiDB_AutoMasterSelector() {
	ms := mdb.AutoMasterSelector(10 * time.Second)

	tx, err := mdb.MasterTx(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer ms.CheckErr(tx.Rollback())

	_, err = tx.Exec("insert something")
	if ms.CheckErr(err) != nil {
		// If error is not nil and not in whitelist,
		// mdb.SelectMaster will be run in a Go routine

		log.Println(err)
		return
	}

	err = ms.CheckErr(tx.Commit())
	if err != nil {
		log.Println(err)
		return
	}

}

func ExampleMultiDB_Master() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db, err := mdb.Master(ctx)
	if err != nil {
		panic(err)
	}

	_, err = db.ExecContext(ctx, "insert something")
	if err != nil {
		panic(err)
	}
}

func ExampleMultiDB_MasterTx() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, err := mdb.MasterTx(ctx, &sql.TxOptions{})
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, "insert something")
	if err != nil {
		panic(err)
	}

	err = tx.Commit()
	if err != nil {
		panic(err)
	}
}

func ExampleMultiDB_MultiNode() {
	mn, err := mdb.MultiNode(2, func(err error) { fmt.Println(err) })
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var i int

	err = mn.QueryRowContext(ctx, "select 1").Scan(&i)
	if err != nil {
		panic(err)
	}
}

func ExampleMultiDB_MultiTx() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mtx, err := mdb.MultiTx(ctx, nil, 2, func(err error) { fmt.Println(err) })
	if err != nil {
		log.Fatal(err)
	}
	defer mtx.Rollback()

	var i int

	err = mtx.QueryRowContext(ctx, "select 1").Scan(&i)
	if err != nil {
		panic(err)
	}
}
