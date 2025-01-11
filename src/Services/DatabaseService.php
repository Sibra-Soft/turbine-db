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
        $this->SetConnection();

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
    public function GetDataset(string $query = null){
        $dataset = array();

        if($query == null){
            $query = $this->query;
        }

        $queryToExecute = $this->doParameterReplacements($query);

        try {
            $statement = $this->dbConnection->prepare($queryToExecute);

            if($statement->execute()){
                array_push($dataset, ["result" => true]);
            }else{
                array_push($dataset, ["result" => false]);
            }

            $statement->setFetchMode(PDO::FETCH_ASSOC);
            $dataset = $statement->fetchAll();
        }
        catch(PDOException $e)
        {
            if($e->getMessage() !== "SQLSTATE[HY000]: General error") throw new Exception($e->errorInfo[2]."#".$queryToExecute);
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

    /**
     * Update a table based on the specified parameters and table
     * @param string $table The table you want to update or insert
     * @param int|null $id The id of the row you want to update
     * @param bool $ignoreDuplicates Tells if you want to ignore duplicates when updating
     * @return bool|int|mixed|string Id of the row you haved updated or the newRowId when you inserted data
     */
    public function UpdateOrInsertRecordBasedOnParameters(string $table, int $id = null, bool $ignoreDuplicates = false):int {
        $this->SetConnection();

        $this->queryBuilder->ignoreDuplicatesOnInsert = $ignoreDuplicates;
        $this->queryBuilder->Clear();

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