<?php
namespace TurbineDb\Services;

use InvalidArgumentException;

use TurbineDb\Enums\QueryOperatorsEnum;
use TurbineDb\Helpers\StringHelpers;
use TurbineDb\Interfaces\IQueryBuilderService;

class QueryBuilderService implements IQueryBuilderService
{
    protected $queryConstructor = [];
    protected $tables = [];

    public bool $ignoreDuplicatesOnInsert = false;

    /**
     * Start with a empty querybuilder
     */
    public function NewQuery(){
        $this->tables = [];
        $this->queryConstructor = [
            "0_STATEMENT" => "SELECT",
            "1_DATA" => ["SELECT" => "*"],
            "2_FROM" => "",
            "3_JOINS" => [],
            "4_WHERE" => [],
            "5_GROUP_BY" => "",
            "6_ORDER_BY" => "",
            "7_LIMIT" => 0
        ];
    }

    /**
     * This function will insert a new record in the specified table
     * @param string $table The target table
     * @param string $column The column you want to fill with the specified value
     * @param string $value The value you want to insert
     */
    public function Insert(string $table, string $column, $value){
        // Reset the data constructor if the last statement was not a select
        if($this->queryConstructor["0_STATEMENT"] !== "INSERT"){$this->queryConstructor["1_DATA"] = [];}

        $this->queryConstructor["0_STATEMENT"] = "INSERT";
        $this->queryConstructor["1_DATA"][$column] = $value;
        $this->queryConstructor["2_FROM"] = $table;
    }

    /**
     * This function can be used to delete a record from a specified table
     * @param string $table
     */
    public function Delete(string $table){
        // Reset the data constructor if the last statement was not a select
        if($this->queryConstructor["0_STATEMENT"] !== "DELETE"){$this->queryConstructor["1_DATA"] = [];}

        $this->queryConstructor["0_STATEMENT"] = "DELETE";
        $this->queryConstructor["2_FROM"] = $table;
    }

    /**
     * This function updates specified columns and values in a table
     * @param string $table The table you want to update
     * @param string $column The column of the table you want to update
     * @param string $value The value you want to update in the specified table and column
     */
    public function Update(string $table, string $column, $value){
        // Reset the data constructor if the last statement was not a select
        if($this->queryConstructor["0_STATEMENT"] !== "UPDATE"){$this->queryConstructor["1_DATA"] = [];}

        $this->queryConstructor["0_STATEMENT"] = "UPDATE";
        $this->queryConstructor["1_DATA"][$column] = $value;
        $this->queryConstructor["2_FROM"] = $table;
    }

    /**
     * This function selects data from specified columns in a specified table
     * @param string $table The table you want to select
     * @param string $columns The columns you want to return
     */
    public function Select(string $table, string $columns = "*"){
        $this->queryConstructor["0_STATEMENT"] = "SELECT";
        $this->queryConstructor["1_DATA"]["SELECT"] = $columns;
        $this->queryConstructor["2_FROM"] = $this->ConstructAlias($table);

        $this->tables[] = $this->ConstructAlias($table);
    }

    /**
     * This function adds a where statement to the query
     * @param integer $extender
     * @param string $column
     * @param string $operator
     * @param string $value
     */
    public function Where(int $extender, string $column, string $operator, string $value){
        $extendWith = "";

        if($extender === 0){
            $extendWith = " AND";
        }elseif($extender === 1){
            $extendWith = " OR";
        }elseif($extender === 2){
            $extendWith = "";
        }

        $whereStatementCount = count((array)$this->queryConstructor["4_WHERE"]);
        $this->queryConstructor["4_WHERE"][$whereStatementCount.";".$extendWith] = [$column.";".$operator => $value];
    }

    /**
     * This function adds a order statement to the query
     * @param string $column
     * @param string $direction
     */
    public function Order(string $column, string $direction){
        $this->queryConstructor["6_ORDER_BY"] = [$direction => $column];
    }

    /**
     * This function adds a limit to the query
     * If you use the limit function you can't use the pagination function
     * @param integer $maxItems The amount of items to display, `0` means unlimited
     */
    public function Limit(int $maxItems = 0){
        $this->queryConstructor["7_LIMIT"] = $maxItems;
    }

    /**
     * This function adds pagination to the query
     * If you use the pagination function you can't use the limit function
     * @param integer $Amount
     * @param integer $Page
     */
    public function Pagination(int $amount, int $page){
        $pageEndAmount = ($page - 1) * $amount;

        $this->queryConstructor["7_LIMIT"] = $pageEndAmount.",".$amount;
    }

    /**
     * Add a table as InnerJoin within the query
     * @param string $table Name of the table and alias (example `table:alias`)
     * @param array $criteria Array of criteria to use for the join
     * @throws InvalidArgumentException When no alias is specified
     */
    public function Join(string $table, array $criteria){
        if(!StringHelpers::Contains($table, ":")) throw new InvalidArgumentException("Tables must is aliased when using Join or LeftJoin");

        $this->queryConstructor["3_JOINS"]["INNER"][$table] = $criteria;

        $this->tables[] = $this->ConstructAlias($table);
    }

    /**
     * Add a table as LeftJoin within the query
     * @param string $table Name of the table and alias (example `table:alias`)
     * @param array $criteria Array of criteria to use for the join
     * @throws InvalidArgumentException When no alias is specified
     */
    public function LeftJoin(string $table, array $criteria){
        if(!StringHelpers::Contains($table, ":")) throw new InvalidArgumentException("Tables must is aliased when using Join or LeftJoin");

        $this->queryConstructor["3_JOINS"]["LEFT"][$table] = $criteria;

        $this->tables[] = $this->ConstructAlias($table);
    }

    /**
     * Construct the table and a alias if given
     * @param string $value Value containing the table an possible alias
     * @return string The tablename and alias if given
     */
    private function ConstructAlias(string $value): string {
        $alias = "";
        $table = "";

        if(StringHelpers::Contains($value, ":")) {
            $alias = " AS ".StringHelpers::SplitString($value, ":", 1);
            $table = StringHelpers::SplitString($value, ":", 0);
        }else{
            $table = $value;
        }

        return $table.$alias;
    }

    /**
     * This function constructs the where clause based on the specified where statements
     * @return string
     */
    private function ConstructWhereClause(): string {
        $constructor = "";

        // Check if where statements have been added
        if(count((array)$this->queryConstructor["4_WHERE"]) > 0){
            $index = 0;
            foreach((array)$this->queryConstructor["4_WHERE"] as $where){
                $column = StringHelpers::SplitString(key($where), ";", 0);
                $value = $where[key($where)];
                $operator = StringHelpers::SplitString(key($where), ";", 1);

                if($index == 0){
                    $constructor .= "WHERE";
                }else{
                    $key = array_keys((array)$this->queryConstructor["4_WHERE"])[$index];
                    $extender = trim(StringHelpers::SplitString($key, ";", 1));

                    if($extender === "AND"){
                        $constructor .= " ";
                        $constructor .= "AND";
                    }else{
                        $constructor .= " ";
                        $constructor .= "OR";
                    }
                }

                $constructor .= " ";

                // Check for specific operators
                switch($operator){
                    case QueryOperatorsEnum::Like:
                        $constructor .= "`$column` LIKE '%$value%'";
                        break;

                    default:
                        $constructor .= "`$column` $operator '$value'";
                        break;
                }

                $index++;
            }
        }

        return $constructor;
    }

    /**
     * This function constructs the select statements
     * @return string
     */
    private function ConstructSelectQuery(): string {
        $constructor = "";

        $selectColumns = $this->queryConstructor["1_DATA"]["SELECT"];

        // Get all the columns to return
        if($selectColumns === "*" and count($this->tables) > 1){
            $selectColumns = "";

            foreach($this->tables as $table){
                $aliasName = trim(StringHelpers::SplitString($table, "AS", 1));

                $selectColumns .= $aliasName.".*,";
            }

            $selectColumns = rtrim($selectColumns, ',');
        }

        // Construct the query
        $constructor .= "SELECT ".$selectColumns;
        $constructor .= " ";
        $constructor .= "FROM ".$this->queryConstructor["2_FROM"];

        // Construct the joins of the query
        foreach($this->queryConstructor["3_JOINS"] as $type => $joins){
            switch($type){
                case "INNER":
                    foreach($joins as $table => $criteria){
                        $constructor .= " INNER JOIN ".$this->ConstructAlias($table);
                        $constructor .= " ON ";

                        $i = 0;
                        foreach($criteria as $column => $value){
                            if($i > 0){
                                $constructor .= " AND ".$column." = ".$value;
                            }else{
                                $constructor .= $column." = ".$value;
                            }

                            $i++;
                        }
                    }
                    break;

                case "LEFT":
                    foreach($joins as $table => $criteria){
                        $constructor .= " LEFT JOIN ".$this->ConstructAlias($table);
                        $constructor .= " ON ";

                        $i = 0;
                        foreach($criteria as $column => $value){
                            if($i > 0){
                                $constructor .= " AND ".$column." = ".$value;
                            }else{
                                $constructor .= $column." = ".$value;
                            }

                            $i++;
                        }
                    }
                    break;
            }
        }

        // Construct the where part of the query
        $constructor .= " ";
        $constructor .= $this->ConstructWhereClause();

        // Add GroupBy, Order, Limit, etc.
        $constructor .= " ";

        // Check if we must add a GroupBy
        if(!StringHelpers::IsNullOrWhiteSpace($this->queryConstructor["5_GROUP_BY"])){
            $constructor .= "GROUP BY ".$this->queryConstructor["5_GROUP_BY"];
            $constructor .= " ";
        }

        // Check if we must add a OrderBy
        if(!StringHelpers::IsNullOrWhiteSpace($this->queryConstructor["6_ORDER_BY"])){
            $orderByDirection = key((array)$this->queryConstructor["6_ORDER_BY"]);
            $orderByColumn = $this->queryConstructor["6_ORDER_BY"][$orderByDirection];

            $constructor .= "ORDER BY ".$orderByColumn." ".$orderByDirection;
            $constructor .= " ";
        }

        if($this->queryConstructor["7_LIMIT"] > 0){
            $constructor .= "LIMIT ".$this->queryConstructor["7_LIMIT"];
        }

        return $constructor;
    }

    /**
     * This function constructs the insert statement
     * @return string
     */
    private function ConstructInsertQuery(): string {
        $constructor = "";

        // Construct the query
        if($this->ignoreDuplicatesOnInsert){
            $constructor .= "INSERT IGNORE INTO ".$this->queryConstructor["2_FROM"];
        }else{
            $constructor .= "INSERT INTO ".$this->queryConstructor["2_FROM"];
        }

        $constructor .= " ";

        $columns = "(";
        $values = "(";
        foreach($this->queryConstructor["1_DATA"] as $column => $value){
            $columns .= "`$column`,";

            // Check if the value is a string, otherwise just add the value to the query
            if(gettype($value) == "string"){
                $values .= "'".addslashes($value)."',";
            }else{
                $values .= $value.",";
            }
        }

        $constructor .= rtrim($columns, ',').")";
        $constructor .= " VALUES ";
        $constructor .= rtrim($values, ',').")";

        return $constructor;
    }

    /**
     * This function constructs the update statement
     * @return string
     */
    private function ConstructUpdateQuery(): string {
        $constructor = "";

        // Construct the query
        $constructor .= "UPDATE ".$this->queryConstructor["2_FROM"];
        $constructor .= " ";

        $values = "";
        $updates = "SET ";
        foreach($this->queryConstructor["1_DATA"] as $column => $value){
            if(gettype($value) == "string"){
                $values .= $updates .= "`$column` = '".addslashes($value)."',";
            }else{
                $updates .= "`$column` = $value,";
            }
        }

        $constructor .= rtrim($updates, ",");
        $constructor .= " ";
        $constructor .= $this->ConstructWhereClause();

        return $constructor;
    }

    /**
     * This function constructs the delete query
     * @return string
     */
    private function ConstructDeleteQuery(): string {
        $constructor = "";

        $constructor .= "DELETE FROM ".$this->queryConstructor["2_FROM"]." ";
        $constructor .= $this->ConstructWhereClause();

        if(count((array)$this->queryConstructor["4_WHERE"]) == 0){
            echo("You must specify a where statement when using the delete function");
            exit();
        }

        return $constructor;
    }

    /**
     * This function will render the query and all it's specified statements
     * @return string
     */
    public function Render(): string {
        $constructor = "";

        // Check if a table is specified
        if(StringHelpers::IsNullOrWhiteSpace($this->queryConstructor["2_FROM"])){
            throw new InvalidArgumentException("No table is specified");
        }

        // Check the statement and run the correct constructor
        switch($this->queryConstructor["0_STATEMENT"]){
            case "SELECT": $constructor = $this->ConstructSelectQuery(); break;
            case "INSERT": $constructor = $this->ConstructInsertQuery(); break;
            case "UPDATE": $constructor = $this->ConstructUpdateQuery(); break;
            case "DELETE": $constructor = $this->ConstructDeleteQuery(); break;
        }

        return $constructor;
    }
}
?>