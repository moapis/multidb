package sqlboiler

import (
	"github.com/moapis/multidb"
	"github.com/volatiletech/sqlboiler/v4/boil"
)

func _() boil.Executor        { return multidb.MultiNode{} }
func _() boil.ContextExecutor { return multidb.MultiNode{} }

func _() boil.Executor          { return &multidb.MultiTx{} }
func _() boil.ContextExecutor   { return &multidb.MultiTx{} }
func _() boil.Transactor        { return &multidb.MultiTx{} }
func _() boil.ContextTransactor { return &multidb.MultiTx{} }

func _() boil.Executor        { return &multidb.Node{} }
func _() boil.ContextExecutor { return &multidb.Node{} }
func _() boil.Beginner        { return &multidb.Node{} }
func _() boil.ContextBeginner { return &multidb.Node{} }
