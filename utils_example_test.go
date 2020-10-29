package multidb

import (
	"context"
	"log"
	"time"

	"github.com/moapis/multidb/drivers/postgresql"
)

func ExampleAutoMasterSelector() {
	mdb := &MultiDB{
		MasterFunc: IsMaster(postgresql.MasterQuery),
	}

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
