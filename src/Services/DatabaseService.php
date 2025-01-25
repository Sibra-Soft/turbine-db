<?php
namespace TurbineDb\Services;

require_once ('vendor/autoload.php'); // Composer vendor packages

use DateTime;
use Exception;
use PDO;
use PDOException;
use PDOStatement;

use TurbineDb\Enums\DataTypesEnum;
use TurbineDb\Enums\QueryExtenderEnum;
use TurbineDb\Enums\QueryOperatorsEnum;
use TurbineDb\Helpers\StringHelpers;

use Noodlehaus\Config;
use ReflectionClass;

class DatabaseService {
    protected $dbConnection;
    protected array $datasetFirstRow = [];
    protected bool $connectionSet = false;
    protected bool $isSqLite = false;
    public static $DatabaseConnection;

    public $rowCount = 0;
    public $rowsAffected = 0;
    public $rowInsertId = 0;
    public $mysqlQueryParameters = [];

    private QueryBuilderService $queryBuilder;
    private $query = "";
    private $lastErrorCode = null;

    public function __construct(){
        $this->queryBuilder = new QueryBuilderService();
    }

    public function SetSQLiteConnection(string $filename){
        if(!$this->connectionSet){
            if(file_exists($filename)){
                // Build the connectionstring
                $conString = "sqlite:{$filename}";

                $this->connectionSet = true;
                $this->isSqLite = true;

                try {
                    $this->dbConnection = new PDO($conString);
                    $this->dbConnection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
                }
                catch(PDOException $e)
                {
                    throw new Exception("SQLite connection error: ".$e->getMessage());
                }
            }else{
                throw new Exception("Database file not found: ".$filename);
            }
        }
    }

    /**
     * Set a new connection based on the configuration settings
     * @param string $name The name of the connection
     * @param bool $force Set if you want to force a new connection
     * @throws Exception Error when connecting to database
     */
    public function SetMysqlConnection(string $name = "default", bool $force = false){
        if(!$this->connectionSet or $force){
            // Load configuration
            $conf = Config::load("config.json");

            // Get the settings from the configuration file
            $server = $conf->get("connections.{$name}.host");
            $port = $conf->get("connections.{$name}.port");
            $database = $conf->get("connections.{$name}.database");
            $username = $conf->get("connections.{$name}.username");
            $password = $conf->get("connections.{$name}.password");

            // Build the connectionstring
            $conString = "mysql:host={$server};dbname={$database};port={$port}";

            $this->connectionSet = true;

            try {
                $this->dbConnection = new PDO($conString, $username, $password);
                $this->dbConnection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            }
            catch(PDOException $e)
            {
                throw new Exception("Mysql connection error: ".$e->getMessage());
            }
        }
    }

    /**
     * Set or change the database based on the database name
     * @param string $name The name of the database you want to use
     */
    public function SetDatabase(string $name){
        $this->ExecuteQuery("USE ".$name);
    }

    /**
     * Get a column value of the first row
     * @param string $column The name of the column
     * @param DataTypesEnum $typeOf The datatype to use for the value
     * @return bool|DateTime|int|string The value of the specified column within the first row of the results
     */
    public function DatasetFirstRow(string $column, DataTypesEnum $typeOf){
        $return = "";

        if($typeOf == DataTypesEnum::TypeString){
            $return = (string)$this->datasetFirstRow[$column];
        }elseif($typeOf == DataTypesEnum::TypeDateTime){
            $return = new DateTime($this->datasetFirstRow[$column]);
        }elseif($typeOf == DataTypesEnum::TypeInteger){
            $return = (integer)$this->datasetFirstRow[$column];
        }elseif($typeOf == DataTypesEnum::TypeBoolean){
            $return = (boolean)$this->datasetFirstRow[$column];
        }

        return $return;
    }

    /**
     * Execute a query
     * @param string $query The query you want to execute
     * @throws Exception When a error occurs when running the query
     * @return bool|PDOStatement Object containing details of the execution
     */
    public function ExecuteQuery(string $query): PDOStatement {
        $this->SetConnection();

        $queryToExecute = $this->doParameterReplacements($query);
        $this->query = $queryToExecute;

        try {
            $statement = $this->dbConnection->query($queryToExecute);

            $this->rowsAffected = $statement->rowCount();
            $this->rowInsertId = $this->dbConnection->lastInsertId();

            return $statement;
        }
        catch(PDOException $e) {
            throw new Exception('Error during Mysql query execution, error: '.print_r($e->errorInfo).' current query: '.$this->query);
        }
    }

    /**
     * Get the fields of a table or query
     * @param string $tableOrQuery The table or the query you want to get the fields of
     * @param string $type The type (table, query)
     * @return array Array containing the details of the fields
     */
    public function GetFieldset(string $tableOrQuery, string $type = "table"){
        $this->SetMysqlConnection();

        $fieldset = array();
        $index = 0;

        if($type == "table"){
            $dataset = $this->getDataset("DESCRIBE ".$tableOrQuery);

            foreach ($dataset as $row) {
                list($type, $length) = explode("(", $row["Type"]."(");

                $fieldset[$row["Field"]] = array(
                    "name" => $row["Field"],
                    "type" => $type,
                    "length" => str_replace(")", "", $length)
                );

                $index++;
            }
        }else{
            $query = $this->DoParameterReplacements($tableOrQuery);
            $queryResult = $this->dbConnection->query($query);

            for ($i = 0; $i < $queryResult->columnCount(); $i++) {
                $col = $queryResult->getColumnMeta($i);
                $columns[] = $col['name'];
            }

            $fieldset = $columns;
        }

        return $fieldset;
    }

    /**
     * Clear all query parameters
     */
    public function ClearParameters(){
        $this->mysqlQueryParameters = array();
    }

    /**
     * Add a new query parameter
     * @param string $name The name of the parameter
     * @param mixed $value The value of the parameter
     * @param mixed $default The default value of the parameter
     */
    public function AddParameter(string $name, $value, $default = null){
        if($value == ""){
            $this->mysqlQueryParameters["?$name"] = $default;
        }else{
            $this->mysqlQueryParameters["?$name"] = $value;
        }
    }

    /**
     * Do the replacements of the query parameters within the specified query
     * @param string $query The query to do the replacements
     * @return string The query containing the replaced parameters with values
     */
    private function DoParameterReplacements(string $query){
        $tempQuery =  $query;

        foreach($this->mysqlQueryParameters as $name => $value){
            if(gettype($value) == "string"){
                $tempQuery = str_replace($name, "'".addslashes($value)."'", $tempQuery);
            }else{
                $tempQuery = str_replace($name, $value, $tempQuery);
            }
        }

        return $tempQuery;
    }

    private function GetTables(string $query): array {
        $tables = [];
        $pattern = '/(FROM|JOIN)\s+([`]?[a-zA-Z0-9_]+[`]?)(?:\s+AS\s+([a-zA-Z0-9_]+)|\s+([a-zA-Z0-9_]+))/';

        preg_match_all($pattern, $query, $matches, PREG_SET_ORDER);

        foreach($matches as $match){
            if(StringHelpers::IsNullOrWhiteSpace($match[3])){
                $tableName = str_replace("`", "", $match[2]);

                $tables[$tableName] = $tableName;
            }else{
                $tableName = str_replace("`", "", $match[2]);

                $tables[$match[3]] = $tableName;
            }
        }

        return $tables;
    }

    /**
     * Get a dataset in JSON format
     * @param string $query The query to execute
     * @return array Array containing the result of the query
     */
    public function GetDatasetAsJson(string $query): array {
        return $this->GetDataset($query);
    }

    /**
     * Get a dataset based on a query
     * @param string|null $query The query to execute
     * @throws Exception When a error occurs running the query
     * @return array The result of the query
     */
    public function GetDataset(?string $query = null){
        $dataset = [];

        $execQuery = $this->doParameterReplacements($query);
        $statement = $this->dbConnection->prepare($execQuery);

        $statement->execute();

        foreach($statement->fetchAll(PDO::FETCH_NUM) as $row) {
            $i = 0;
            $data = [];

            foreach($row as $key => $value){
                $meta = $statement->getColumnMeta($i);

                $table = "";
                $tables = $this->GetTables($execQuery);

                if(count($tables) > 1){
                    if(array_key_exists("table", $meta)){
                        $table = array_search($meta["table"], $tables).".";

                        if($table === ".") $table = "";
                    }
                }

                $name = $table.$meta["name"];
                $data[$name] = $value;

                $i++;
            }

            array_push($dataset, $data);
        }

        // Get the rowcount
        if($this->isSqLite){
            $this->rowCount = count($dataset);
        }else{
            $this->rowCount = $statement->rowCount();
        }

        // Only set the datasetfirstrow property if a row is returend
        if($this->rowCount > 0) $this->datasetFirstRow = $dataset[0];

        return $dataset;
    }

    private function GetObjectPropertiesWithTypes(object $obj): array {
        $reflection = new ReflectionClass($obj);
        $properties = $reflection->getProperties();
        $result = [];

        foreach ($properties as $property) {
            $property->setAccessible(true);
            $type = $property->getType();

            $result[$property->getName()] = $type ? $type->getName() : 'unknown';
        }

        return $result;
    }

    /**
     * Executes a query and returns a array containing models of the given model
     * @template T
     * @param string $query The query to execute
     * @param string|class-string<T> $model The model to use
     * @return T[]
     */
    public function LisOf(string $query, string $model): mixed {
        $dataset = $this->GetDataset($query);
        $returnModel = [];

        foreach($dataset as $row){
            $class = new $model;
            $properties = $this->GetObjectPropertiesWithTypes($class);

            foreach($properties as $name => $type){
                $tables = $this->GetTables($query);
                $column = $name;

                foreach(array_keys($tables) as $table){
                    if(str_contains($name, $table)){
                        $column = substr_replace($name, ".", strlen($table), 1);
                    }
                }

                $value = $row[$column];

                if($type === "string"){
                    $value = (string)$value;
                }elseif($type === "int"){
                    $value = (int)$value;
                }elseif($type === "DateTime"){
                    if(is_null($value)){
                        $value = null;
                    }else{
                        $value = (new DateTime($value));
                    }
                }elseif($type === "bool"){
                    $value = (bool)$value;
                }

                $class->$name = $value;
            }

            array_push($returnModel, $class);
        }

        return $returnModel;
    }

    /**
     * Update a table based on the specified parameters and table
     * @param string $table The table you want to update or insert
     * @param int|null $id The id of the row you want to update
     * @param bool $ignoreDuplicates Tells if you want to ignore duplicates when updating
     * @return int Id of the row you haved updated or the newRowId when you inserted data
     */
    public function UpdateOrInsertRecordBasedOnParameters(string $table, ?int $id = null, bool $ignoreDuplicates = false): int {
        $this->queryBuilder->ignoreDuplicatesOnInsert = $ignoreDuplicates;
        $this->queryBuilder->NewQuery();

        // Generate the query based on the specified parameters
        $parameters = $this->mysqlQueryParameters;

        // Check if it's a add or a update query
        if(StringHelpers::IsNullOrWhiteSpace($id)){
            foreach($parameters as $key => $value){
                $this->queryBuilder->Insert($table, str_replace("?", "", $key), $value);
            }
        }else{
            foreach($parameters as $key => $value){
                $this->queryBuilder->Update($table, str_replace("?", "", $key), $value);
            }
            $this->queryBuilder->Where(QueryExtenderEnum::Nothing, "id", QueryOperatorsEnum::EqualTo, $id);

        }

        // Render and execute the query
        $query = $this->queryBuilder->Render();
        $this->ExecuteQuery($query);

        return $this->rowInsertId;
    }
}
?>