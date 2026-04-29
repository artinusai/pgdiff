//
// Copyright (c) 2017 Jon Carlson.  All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.
//

package main

import (
	"fmt"
	"log"

	"os"
	"strings"

	flag "github.com/ogier/pflag"

	"github.com/joncrlsn/pgutil"
	_ "github.com/lib/pq"
)

// Schema is a database definition (table, column, constraint, indes, role, etc) that can be
// added, dropped, or changed to match another database.
type Schema interface {
	debug()
	Compare(schema interface{}) int
	Add()
	Drop()
	Change(schema interface{})
	NextRow() bool
}

const (
	version = "0.9.3"
)

var (
	args       []string
	dbInfo1    pgutil.DbInfo
	dbInfo2    pgutil.DbInfo
	schemaType string
)

/*
 * Initialize anything needed later
 */
func init() {
}

/*
 * Do the main logic
 */
func main() {
	var helpPtr = flag.BoolP("help", "?", false, "print help information")
	var planPtr = flag.Bool("plan", false, "print the execution plan for the selected schema type and exit")
	var versionPtr = flag.BoolP("version", "V", false, "print version information")

	dbInfo1, dbInfo2 = parseFlags()

	// Remaining args:
	args = flag.Args()

	if *helpPtr {
		usage()
	}

	if *versionPtr {
		fmt.Fprintf(os.Stderr, "%s - version %s\n", os.Args[0], version)
		fmt.Fprintln(os.Stderr, "Copyright (c) 2017 Jon Carlson.  All rights reserved.")
		fmt.Fprintln(os.Stderr, "Use of this source code is governed by the MIT license")
		fmt.Fprintln(os.Stderr, "that can be found here: http://opensource.org/licenses/MIT")
		os.Exit(1)
	}

	if len(args) == 0 {
		if *planPtr {
			args = []string{"ALL"}
		} else {
			fmt.Println("The required first argument is SchemaType:", schemaTypesText())
			os.Exit(1)
		}
	}

	// Verify schemas
	schemas := dbInfo1.DbSchema + dbInfo2.DbSchema
	if schemas != "**" && strings.Contains(schemas, "*") {
		fmt.Println("If one schema is an asterisk, both must be.")
		os.Exit(1)
	}

	schemaType = strings.ToUpper(args[0])
	plan, err := buildExecutionPlan(schemaType, dbInfo1, dbInfo2)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if *planPtr {
		printExecutionPlan(plan)
		return
	}

	fmt.Println("-- schemaType:", schemaType)

	// fmt.Println("-- db1:", dbInfo1)
	// fmt.Println("-- db2:", dbInfo2)
	fmt.Println("-- Run the following SQL against db2:")

	conn1, err := dbInfo1.Open()
	check("opening database 1", err)

	conn2, err := dbInfo2.Open()
	check("opening database 2", err)

	runExecutionPlan(conn1, conn2, plan)
}

/*
 * This is a generic diff function that compares tables, columns, indexes, roles, grants, etc.
 * Different behaviors are specified the Schema implementations
 */
func doDiff(db1 Schema, db2 Schema) {

	more1 := db1.NextRow()
	more2 := db2.NextRow()
	for more1 || more2 {
		// fmt.Println(">>>>>>>>> ----------- ")
		// db1.debug()

		compareVal := db1.Compare(db2)
		if compareVal == 0 {
			// fmt.Println(">>>>>>> CompareVal == 0")
			// table and column match, look for non-identifying changes
			db1.Change(db2)
			// table and column match, look for non-identifying changes
			more1 = db1.NextRow()
			more2 = db2.NextRow()
		} else if compareVal < 0 {
			// fmt.Println(">>>>>>> CompareVal < 0")
			// db2 is missing a value that db1 has
			if more1 {
				// fmt.Println(">>>>>> Add")
				db1.Add()
				more1 = db1.NextRow()
			} else {
				// fmt.Println(">>>>>> Drop")
				// db1 is at the end
				db2.Drop()
				more2 = db2.NextRow()
			}
		} else if compareVal > 0 {
			// fmt.Println(">>>>>>> CompareVal > 0")
			// db2 has an extra column that we don't want
			if more2 {
				db2.Drop()
				more2 = db2.NextRow()
			} else {
				// db2 is at the end
				db1.Add()
				more1 = db1.NextRow()
			}
		}

		// fmt.Println("----------- <<<<<<<<< ")
		// os.Exit(0)
	}
	// fmt.Println(">>>>>>> End of do diff")
}

func usage() {
	fmt.Fprintf(os.Stderr, "%s - version %s\n", os.Args[0], version)
	fmt.Fprintf(os.Stderr, "usage: %s [<options>] [<schemaType>] \n", os.Args[0])
	fmt.Fprintln(os.Stderr, `
Compares the schema between two PostgreSQL databases and generates alter statements 
that can be *manually* run against the second database.

Options:
  -?, --help    : print help information
      --plan    : print the execution plan for <schemaType> and exit (defaults to ALL)
  -V, --version : print version information
  -v, --verbose : print extra run information
  -U, --user1   : first postgres user 
  -u, --user2   : second postgres user 
  -H, --host1   : first database host.  default is localhost 
  -h, --host2   : second database host. default is localhost 
  -P, --port1   : first port.  default is 5432 
  -p, --port2   : second port. default is 5432 
  -D, --dbname1 : first database name 
  -d, --dbname2 : second database name 
  -S, --schema1 : first schema.  default is all schemas
  -s, --schema2 : second schema. default is all schemas

<schemaType> can be: `+schemaTypesText()+"\n")

	os.Exit(2)
}

func check(msg string, err error) {
	if err != nil {
		log.Fatal("Error "+msg, err)
	}
}
