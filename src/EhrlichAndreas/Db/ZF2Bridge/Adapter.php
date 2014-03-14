<?php 

//require_once 'EhrlichAndreas/Db/Exception.php';

//require_once 'EhrlichAndreas/Util/Dsn.php';

//require_once 'EhrlichAndreas/Db/Adapter/Pdo/Abstract.php';

/**
 *
 * @author Ehrlich, Andreas <ehrlich.andreas@googlemail.com>
 */
class EhrlichAndreas_Db_ZF2Bridge_Adapter extends EhrlichAndreas_Db_Adapter_Pdo_Abstract
{
    /**
     *
     * @var EhrlichAndreas_Db_Adapter_Abstract 
     */
    protected $adapter = null;
    
	/**
	 * @param \Zend\Db\Adapter\Adapter $adapter
	 */
	public function __construct($adapter)
	{
        $dbConfig = $adapter->getDriver()->getConnection()->getConnectionParameters();
                
        if (isset($dbConfig['dsn']))
        {
            $data = EhrlichAndreas_Util_Dsn::parseDsn($dbConfig['dsn']);

            foreach ($data as $key => $value)
            {
                $dbConfig[$key] = $value;
            }

            $data = EhrlichAndreas_Util_Dsn::parseUri($dbConfig['dsn']);

            if (isset($data[0]))
            {
                $dbConfig['adapter'] = ucfirst($data[0]);
            }

            if (isset($dbConfig['adapter']) && $dbConfig['driver'])
            {
                $dbConfig['adapter'] = $dbConfig['driver'] . '_' . $dbConfig['adapter'];
            }
        }
        
        $this->adapter = EhrlichAndreas_Db_Db::factory($dbConfig);
        
        $this->adapter->setConnection($adapter->getDriver()->getConnection()->getResource());
	}

    /**
     * Leave autocommit mode and begin a transaction.
     *
     * @return EhrlichAndreas_Db_ZF2Bridge_Adapter
     */
    public function beginTransaction ()
    {
        $this->adapter->beginTransaction();
        
        return $this;
    }

    /**
     * Force the connection to close.
     *
     * @return void
     */
    public function closeConnection ()
    {
        $this->adapter->closeConnection();
    }

    /**
     * Commit a transaction and return to autocommit mode.
     *
     * @return EhrlichAndreas_Db_ZF2Bridge_Adapter
     */
    public function commit ()
    {
        $this->adapter->commit();
        
        return $this;
    }

    /**
     * Deletes table rows based on a WHERE clause.
     *
     * @param mixed $table
     *            The table to update.
     * @param mixed $where
     *            DELETE WHERE clause(s).
     * @return int The number of affected rows.
     */
    public function delete ($table, $where = '')
    {
        return $this->adapter->delete($table, $where);
    }

    /**
     * Returns the column descriptions for a table.
     *
     * The return value is an associative array keyed by the column name,
     * as returned by the RDBMS.
     *
     * The value of each array element is an associative array
     * with the following keys:
     *
     * SCHEMA_NAME => string; name of database or schema
     * TABLE_NAME => string;
     * COLUMN_NAME => string; column name
     * COLUMN_POSITION => number; ordinal position of column in table
     * DATA_TYPE => string; SQL datatype name of column
     * DEFAULT => string; default expression of column, null if none
     * NULLABLE => boolean; true if column can have nulls
     * LENGTH => number; length of CHAR/VARCHAR
     * SCALE => number; scale of NUMERIC/DECIMAL
     * PRECISION => number; precision of NUMERIC/DECIMAL
     * UNSIGNED => boolean; unsigned property of an integer type
     * PRIMARY => boolean; true if column is part of the primary key
     * PRIMARY_POSITION => integer; position of column in primary key
     *
     * @param string $tableName
     * @param string $schemaName
     *            OPTIONAL
     * @return array
     */
    public function describeTable ($tableName, $schemaName = null)
    {
        return $this->adapter->describeTable($tableName, $schemaName);
    }

    /**
     * Fetches all SQL result rows as a sequential array.
     * Uses the current fetchMode for the adapter.
     *
     * @param string|EhrlichAndreas_Db_Select|Zend_Db_Select $sql
     *            An SQL SELECT statement.
     * @param mixed $bind
     *            Data to bind into SELECT placeholders.
     * @param mixed $fetchMode
     *            Override current fetch mode.
     * @return array
     */
    public function fetchAll ($sql, $bind = array(), $fetchMode = null)
    {
        return $this->adapter->fetchAll($sql, $bind, $fetchMode);
    }

    /**
     * Fetches all SQL result rows as an associative array.
     *
     * The first column is the key, the entire row array is the
     * value. You should construct the query to be sure that
     * the first column contains unique values, or else
     * rows with duplicate values in the first column will
     * overwrite previous data.
     *
     * @param string|EhrlichAndreas_Db_Select|Zend_Db_Select $sql
     *            An SQL SELECT statement.
     * @param mixed $bind
     *            Data to bind into SELECT placeholders.
     * @return array
     */
    public function fetchAssoc ($sql, $bind = array())
    {
        return $this->adapter->fetchAssoc($sql, $bind);
    }

    /**
     * Fetches the first column of all SQL result rows as an array.
     *
     * @param string|EhrlichAndreas_Db_Select|Zend_Db_Select $sql
     *            An SQL SELECT statement.
     * @param mixed $bind
     *            Data to bind into SELECT placeholders.
     * @return array
     */
    public function fetchCol ($sql, $bind = array())
    {
        return $this->adapter->fetchCol($sql, $bind);
    }

    /**
     * Fetches the first column of the first row of the SQL result.
     *
     * @param string|EhrlichAndreas_Db_Select|Zend_Db_Select $sql
     *            An SQL SELECT statement.
     * @param mixed $bind
     *            Data to bind into SELECT placeholders.
     * @return string
     */
    public function fetchOne ($sql, $bind = array())
    {
        return $this->adapter->fetchOne($sql, $bind);
    }

    /**
     * Fetches all SQL result rows as an array of key-value pairs.
     *
     * The first column is the key, the second column is the
     * value.
     *
     * @param string|EhrlichAndreas_Db_Select|Zend_Db_Select $sql
     *            An SQL SELECT statement.
     * @param mixed $bind
     *            Data to bind into SELECT placeholders.
     * @return array
     */
    public function fetchPairs ($sql, $bind = array())
    {
        return $this->adapter->fetchPairs($sql, $bind);
    }

    /**
     * Fetches the first row of the SQL result.
     * Uses the current fetchMode for the adapter.
     *
     * @param string|EhrlichAndreas_Db_Select|Zend_Db_Select $sql
     *            An SQL SELECT statement.
     * @param mixed $bind
     *            Data to bind into SELECT placeholders.
     * @param mixed $fetchMode
     *            Override current fetch mode.
     * @return array
     */
    public function fetchRow ($sql, $bind = array(), $fetchMode = null)
    {
        return $this->adapter->fetchRow($sql, $bind, $fetchMode);
    }

    /**
     * Helper method to change the case of the strings used
     * when returning result sets in FETCH_ASSOC and FETCH_BOTH
     * modes.
     *
     * This is not intended to be used by application code,
     * but the method must be public so the Statement class
     * can invoke it.
     *
     * @param string $key
     * @return string
     */
    public function foldCase ($key)
    {
        return $this->adapter->foldCase($key);
    }

    /**
     * Returns the underlying database connection object or resource.
     * If not presently connected, this initiates the connection.
     *
     * @return EhrlichAndreas_Pdo_Pdo
     */
    public function getConnection ()
    {
        return $this->adapter->getConnection();
    }

    /**
     * Returns the configuration variables in this adapter.
     *
     * @return array
     */
    public function getConfig ()
    {
        return $this->adapter->getConfig();
    }
    /**
     * Get the fetch mode.
     *
     * @return int
     */
    public function getFetchMode ()
    {
        return $this->adapter->getFetchMode();
    }

    /**
     * Returns the symbol the adapter uses for delimited identifiers.
     *
     * @return string
     */
    public function getQuoteIdentifierSymbol ()
    {
        return $this->adapter->getQuoteIdentifierSymbol();
    }

    /**
     * Retrieve server version in PHP style
     *
     * @return string
     */
    public function getServerVersion ()
    {
        return $this->adapter->getServerVersion();
    }

    /**
     * Get the default statement class.
     *
     * @return string
     */
    public function getStatementClass ()
    {
        return $this->adapter->getStatementClass();
    }

    /**
     * Inserts a table row with specified data.
     *
     * @param mixed $table
     *            The table to insert data into.
     * @param array $bind
     *            Column-value pairs.
     * @return int The number of affected rows.
     * @throws EhrlichAndreas_Db_Exception
     */
    public function insert ($table, array $bind)
    {
        return $this->adapter->insert($table, $bind);
    }

    /**
     * Test if a connection is active
     *
     * @return boolean
     */
    public function isConnected ()
    {
        return $this->adapter->isConnected();
    }

    /**
     * Gets the last ID generated automatically by an IDENTITY/AUTOINCREMENT
     * column.
     *
     * As a convention, on RDBMS brands that support sequences
     * (e.g. Oracle, PostgreSQL, DB2), this method forms the name of a sequence
     * from the arguments and returns the last id generated by that sequence.
     * On RDBMS brands that support IDENTITY/AUTOINCREMENT columns, this method
     * returns the last value generated for such a column, and the table name
     * argument is disregarded.
     *
     * @param string $tableName
     *            OPTIONAL Name of table.
     * @param string $primaryKey
     *            OPTIONAL Name of primary key column.
     * @return string
     */
    public function lastInsertId ($tableName = null, $primaryKey = null)
    {
        return $this->adapter->lastInsertId($tableName, $primaryKey);
    }

    /**
     * Return the most recent value from the specified sequence in the database.
     * This is supported only on RDBMS brands that support sequences
     * (e.g. Oracle, PostgreSQL, DB2). Other RDBMS brands return null.
     *
     * @param string $sequenceName
     * @return string
     */
    public function lastSequenceId ($sequenceName)
    {
        return $this->adapter->lastSequenceId($sequenceName);
    }

    /**
     * Adds an adapter-specific LIMIT clause to the SELECT statement.
     *
     * @param mixed $sql
     * @param integer $count
     * @param integer $offset
     * @return string
     */
    public function limit ($sql, $count, $offset = 0)
    {
        return $this->adapter->limit($sql, $count, $offset);
    }

    /**
     * Returns a list of the tables in the database.
     *
     * @return array
     */
    public function listTables ()
    {
        return $this->adapter->listTables();
    }

    /**
     * Generate a new value from the specified sequence in the database, and
     * return it.
     * This is supported only on RDBMS brands that support sequences
     * (e.g. Oracle, PostgreSQL, DB2). Other RDBMS brands return null.
     *
     * @param string $sequenceName
     * @return string
     */
    public function nextSequenceId ($sequenceName)
    {
        return $this->adapter->nextSequenceId($sequenceName);
    }

    /**
     * Prepare a statement and return a PDOStatement-like object.
     *
     * @param string|EhrlichAndreas_Db_Select|Zend_Db_Select $sql
     *            SQL query
     * @return EhrlichAndreas_Db_Statement PDOStatement
     */
    public function prepare ($sql)
    {
        return $this->adapter->prepare($sql);
    }

    /**
     * Prepares and executes an SQL statement with bound data.
     *
     * @param mixed $sql
     *            The SQL statement with placeholders.
     *            May be a string or EhrlichAndreas_Db_Select.
     * @param mixed $bind
     *            An array of data to bind to the placeholders.
     * @return EhrlichAndreas_Db_Statement_Interface
     */
    public function query ($sql, $bind = array())
    {
        return $this->adapter->query($sql, $bind);
    }

    /**
     * Safely quotes a value for an SQL statement.
     *
     * If an array is passed as the value, the array values are quoted
     * and then returned as a comma-separated string.
     *
     * @param mixed $value
     *            The value to quote.
     * @param mixed $type
     *            OPTIONAL the SQL datatype name, or constant, or null.
     * @return mixed An SQL-safe quoted value (or string of separated values).
     */
    public function quote ($value, $type = null)
    {
        return $this->adapter->quote($value, $type);
    }

    /**
     * Quote a column identifier and alias.
     *
     * @param string|array|EhrlichAndreas_Db_Expr|Zend_Db_Expr $ident
     *            The identifier or expression.
     * @param string $alias
     *            An alias for the column.
     * @param boolean $auto
     *            If true, heed the AUTO_QUOTE_IDENTIFIERS config option.
     * @return string The quoted identifier and alias.
     */
    public function quoteColumnAs ($ident, $alias, $auto = false)
    {
        return $this->adapter->quoteColumnAs($ident, $alias, $auto);
    }

    /**
     * Quotes an identifier.
     *
     * Accepts a string representing a qualified indentifier. For Example:
     * <code>
     * $adapter->quoteIdentifier('myschema.mytable')
     * </code>
     * Returns: "myschema"."mytable"
     *
     * Or, an array of one or more identifiers that may form a qualified
     * identifier:
     * <code>
     * $adapter->quoteIdentifier(array('myschema','my.table'))
     * </code>
     * Returns: "myschema"."my.table"
     *
     * The actual quote character surrounding the identifiers may vary depending
     * on
     * the adapter.
     *
     * @param string|array|EhrlichAndreas_Db_Expr|Zend_Db_Expr $ident
     *            The identifier.
     * @param boolean $auto
     *            If true, heed the AUTO_QUOTE_IDENTIFIERS config option.
     * @return string The quoted identifier.
     */
    public function quoteIdentifier ($ident, $auto = false)
    {
        return $this->adapter->quoteIdentifier($ident, $auto);
    }

    /**
     * Quotes a value and places into a piece of text at a placeholder.
     *
     * The placeholder is a question-mark; all placeholders will be replaced
     * with the quoted value. For example:
     *
     * <code>
     * $text = "WHERE date < ?";
     * $date = "2005-01-02";
     * $safe = $sql->quoteInto($text, $date);
     * // $safe = "WHERE date < '2005-01-02'"
     * </code>
     *
     * @param string $text
     *            The text with a placeholder.
     * @param mixed $value
     *            The value to quote.
     * @param string $type
     *            OPTIONAL SQL datatype
     * @param integer $count
     *            OPTIONAL count of placeholders to replace
     * @return string An SQL-safe quoted value placed into the original text.
     */
    public function quoteInto ($text, $value, $type = null, $count = null)
    {
        return $this->adapter->quoteInto($text, $value, $type, $count);
    }

    /**
     * Quote a table identifier and alias.
     *
     * @param string|array|EhrlichAndreas_Db_Expr|Zend_Db_Expr $ident
     *            The identifier or expression.
     * @param string $alias
     *            An alias for the table.
     * @param boolean $auto
     *            If true, heed the AUTO_QUOTE_IDENTIFIERS config option.
     * @return string The quoted identifier and alias.
     */
    public function quoteTableAs ($ident, $alias = null, $auto = false)
    {
        return $this->adapter->quoteInto($ident, $alias, $auto);
    }

    /**
     * Roll back a transaction and return to autocommit mode.
     *
     * @return EhrlichAndreas_Db_ZF2Bridge_Adapter
     */
    public function rollBack ()
    {
        $this->adapter->rollBack();

        return $this;
    }

    /**
     * Set the fetch mode.
     *
     * @param integer $mode
     * @return void
     * @throws EhrlichAndreas_Db_Exception
     */
    public function setFetchMode ($mode)
    {
        return $this->adapter->setFetchMode($mode);
    }

    /**
     * Set the default statement class.
     *
     * @return EhrlichAndreas_Db_ZF2Bridge_Adapter Fluent interface
     */
    public function setStatementClass ($class)
    {
        $this->adapter->setStatementClass($class);

        return $this;
    }

    /**
     * Check if the adapter supports real SQL parameters.
     *
     * @param string $type
     *            'positional' or 'named'
     * @return bool
     */
    public function supportsParameters ($type)
    {
        return $this->adapter->supportsParameters($type);
    }

    /**
     * Updates table rows with specified data based on a WHERE clause.
     *
     * @param mixed $table
     *            The table to update.
     * @param array $bind
     *            Column-value pairs.
     * @param mixed $where
     *            UPDATE WHERE clause(s).
     * @return int The number of affected rows.
     * @throws EhrlichAndreas_Db_Exception
     */
    public function update ($table, array $bind, $where = '')
    {
        return $this->adapter->update($table, $bind, $where);
    }
}

