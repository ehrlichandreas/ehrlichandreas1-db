<?php 

//require_once 'EhrlichAndreas/Db/Exception.php';

//require_once 'EhrlichAndreas/Db/Abstract.php';

//require_once 'EhrlichAndreas/Db/Adapter/Pdo/Abstract/Statement.php';

//require_once 'EhrlichAndreas/Pdo/Pdo.php';

//require_once 'EhrlichAndreas/Util/Object.php';

/**
 * Class for connecting to SQL databases and performing common operations using
 * PDO.
 *
 * @author Ehrlich, Andreas <ehrlich.andreas@googlemail.com>
 */
abstract class EhrlichAndreas_Db_Adapter_Pdo_Abstract extends EhrlichAndreas_Db_Adapter_Abstract
{

    /**
     * PDO type.
     *
     * @var string
     */
    protected $_pdoType = '';

    /**
     * Default class name for a DB statement.
     *
     * @var string
     */
    protected $_defaultStmtClass = 'EhrlichAndreas_Db_Adapter_Pdo_Abstract_Statement';

    /**
     * Creates a PDO DSN for the adapter from $this->_config settings.
     *
     * @return string
     */
    protected function _dsn ()
    {
        // baseline of DSN parts
        $dsn = $this->_config;

        // don't pass the username, password, charset, persistent and
        // driver_options in the DSN
        
        foreach ($dsn as $key => $value)
        {
            if (stripos($key, 'user') !== false)
            {
                unset($dsn[$key]);
            }
            elseif (stripos($key, 'pass') !== false)
            {
                unset($dsn[$key]);
            }
            elseif (stripos($key, 'option') !== false)
            {
                unset($dsn[$key]);
            }
            elseif (stripos($key, 'driver') !== false)
            {
                unset($dsn[$key]);
            }
            elseif (stripos($key, 'persistent') !== false)
            {
                unset($dsn[$key]);
            }
            elseif (stripos($key, 'charset') !== false)
            {
                unset($dsn[$key]);
            }
            elseif (is_array($value))
            {
                unset($dsn[$key]);
            }
        }

        if ($dsn['host'] == 'localhost' && empty($dsn['unix_socket']))
        {
            $dsn['host'] = '127.0.0.1';
        }

        // use all remaining parts in the DSN
        foreach ($dsn as $key => $val)
        {
            $dsn[$key] = "$key=$val";
        }

        return $this->_pdoType . ':' . implode(';', $dsn);
    }

    /**
     * Creates a PDO object and connects to the database.
     *
     * @return void
     * @throws EhrlichAndreas_Db_Exception
     */
    protected function _connect ()
    {
        // if we already have a PDO object, no need to re-connect.
        if ($this->_connection)
		{
            return;
        }

        // get the dsn first, because some adapters alter the $_pdoType
        $dsn = $this->_dsn();

        /*

        $PDOclass = 'PDO';

        // check for PDO extension
        if (! extension_loaded('pdo') || ! extension_loaded($this->_pdoType)) {
            $PDOclass = 'EhrlichAndreas_Pdo';
        }

        // check the PDO driver is available
        if (! in_array($this->_pdoType, call_user_func(array($PDOclass,'getAvailableDrivers')))) {
            /**
             *
             * @see EhrlichAndreas_Db_Exception
             */
        /*    throw new EhrlichAndreas_Db_Exception('The ' . $this->_pdoType . ' driver is not currently installed');
        }

        */
        
        foreach ($this->_config as $key => $value)
        {
            if (stripos($key, 'user') !== false)
            {
                $this->_config['username'] = $value;
            }
            elseif (stripos($key, 'pass') !== false)
            {
                $this->_config['password'] = $value;
            }
            elseif (stripos($key, 'driver') !== false && stripos($key, 'option') !== false)
            {
                if (! isset($this->_config['driver_options']))
                {
                    $this->_config['driver_options'] = $value;
                }
                else
                {
                    // can't use array_merge() because keys might be integers
                    foreach ((array) $value as $key => $val)
                    {
                        $this->_config['driver_options'][$key] = $val;
                    }
                }
            }
            elseif (stripos($key, 'persistent') !== false)
            {
                $this->_config['persistent'] = $value;
            }
            elseif (stripos($key, 'charset') !== false)
            {
                $this->_config['charset'] = $value;
            }
        }

        // create PDO connection
        // $q = $this->_profiler->queryStart('connect',
        // EhrlichAndreas_Db_Profiler::CONNECT);

        // add the persistence flag if we find it in our config array
        if (isset($this->_config['persistent']) && ($this->_config['persistent'] == true))
		{
            $this->_config['driver_options'][EhrlichAndreas_Db_Abstract::ATTR_PERSISTENT] = true;
        }
		
		$pdoClass = '';

        try
		{
            if (extension_loaded('pdo') && in_array($this->_pdoType, PDO::getAvailableDrivers()))
			{
				$pdoClass = 'PDO';
            }
			else
			{
				$pdoClass = 'EhrlichAndreas_Pdo_Pdo';
            }
			
            $this->_connection = new $pdoClass($dsn, $this->_config['username'], $this->_config['password'], $this->_config['driver_options']);

            //$this->_profiler->queryEnd($q);

            // set the PDO connection to perform case-folding on array keys, or
            // not
            $this->_connection->setAttribute(EhrlichAndreas_Db_Abstract::ATTR_CASE, $this->_caseFolding);

            // always use exceptions.
            $this->_connection->setAttribute(EhrlichAndreas_Db_Abstract::ATTR_ERRMODE, EhrlichAndreas_Db_Abstract::ERRMODE_EXCEPTION);
        }
		catch (Exception $e)
		{
            /**
             *
             * @see EhrlichAndreas_Db_Adapter_Exception
             */
            throw new EhrlichAndreas_Db_Exception($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Test if a connection is active
     *
     * @return boolean
     */
    public function isConnected ()
    {
        return ((bool) (EhrlichAndreas_Util_Object::isInstanceOf($this->_connection,'EhrlichAndreas_Pdo_Pdo') || EhrlichAndreas_Util_Object::isInstanceOf($this->_connection,'PDO')));
    }

    /**
     * Force the connection to close.
     *
     * @return void
     */
    public function closeConnection ()
    {
        $this->_connection = null;
    }

    /**
     * Prepares an SQL statement.
     *
     * @param string $sql
     *            The SQL statement with placeholders.
     * @param array $bind
     *            An array of data to bind to the placeholders.
     * @return EhrlichAndreas_Db_Statement
     */
    public function prepare ($sql)
    {
        $this->_connect();

        $stmtClass = $this->_defaultStmtClass;
        
        $stmt = new $stmtClass($this, $sql);
        
        $stmt->setFetchMode($this->_fetchMode);

        return $stmt;
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
     * On RDBMS brands that don't support sequences, $tableName and $primaryKey
     * are ignored.
     *
     * @param string $tableName
     *            OPTIONAL Name of table.
     * @param string $primaryKey
     *            OPTIONAL Name of primary key column.
     * @return string
     */
    public function lastInsertId ($tableName = null, $primaryKey = null)
    {
        $this->_connect();

        return $this->_connection->lastInsertId();
    }

    /**
     * Special handling for PDO query().
     * All bind parameter names must begin with ':'
     *
     * @param string|EhrlichAndreas_Db_Select|Zend_Db_Select $sql
     *            The SQL statement with placeholders.
     * @param array $bind
     *            An array of data to bind to the placeholders.
     * @return EhrlichAndreas_Db_Adapter_Pdo_Abstract_Statement
     * @throws EhrlichAndreas_Db_Exception To re-throw PDOException.
     */
    public function query ($sql, $bind = array())
    {
        if (empty($bind) && is_object($sql) && method_exists($sql, 'getBind'))
		{
            $bind = $sql->getBind();
        }

        if (is_array($bind))
		{
            foreach ($bind as $name => $value)
			{
                if (! is_int($name) && ! preg_match('/^:/', $name))
				{
                    $newName = ":$name";

                    unset($bind[$name]);

                    $bind[$newName] = $value;
                }
            }
        }

        try
        {
            return parent::query($sql, $bind);
        }
        catch (Exception $e)
        {
            /**
             *
             * @see EhrlichAndreas_Db_Exception
             */
            throw new EhrlichAndreas_Db_Exception($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Executes an SQL statement and return the number of affected rows
     *
     * @param mixed $sql
     *            The SQL statement with placeholders.
     *            May be a string or EhrlichAndreas_Db_Select or Zend_Db_Select.
     * @return integer Number of rows that were modified
     *         or deleted by the SQL statement
     */
    public function exec ($sql)
    {
        if (is_object($sql) && method_exists($sql, 'assemble'))
		{
            $sql = $sql->assemble();
        }

		if (is_object($sql) && method_exists($sql, 'getSqlString'))
		{
			$sql = $sql->getSqlString();
		}

        if (is_object($sql) && method_exists($sql, '__toString') && ! method_exists($sql, 'assemble'))
		{
            $sql = $sql->__toString();
        }

        try
		{
            $affected = $this->getConnection()->exec($sql);

            if ($affected === false)
			{
                $errorInfo = $this->getConnection()->errorInfo();
                /**
                 *
                 * @see EhrlichAndreas_Db_Exception
                 */
                throw new EhrlichAndreas_Db_Exception($errorInfo[2]);
            }

            return $affected;
        }
		catch (Exception $e)
		{
            /**
             *
             * @see EhrlichAndreas_Db_Exception
             */
            throw new EhrlichAndreas_Db_Exception($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Quote a raw string.
     *
     * @param string $value
     *            Raw string
     * @return string Quoted string
     */
    protected function _quote ($value)
    {
        if (is_int($value) || is_float($value))
        {
            return $value;
        }

        $this->_connect();

        return $this->_connection->quote($value);
    }

    /**
     * Begin a transaction.
     */
    protected function _beginTransaction ()
    {
        $this->_connect();
        
        $this->_connection->beginTransaction();
    }

    /**
     * Commit a transaction.
     */
    protected function _commit ()
    {
        $this->_connect();
        
        $this->_connection->commit();
    }

    /**
     * Roll-back a transaction.
     */
    protected function _rollBack ()
    {
        $this->_connect();
        
        $this->_connection->rollBack();
    }

    /**
     * Set the PDO fetch mode.
     *
     * @todo Support FETCH_CLASS and FETCH_INTO.
     *
     * @param int $mode
     *            A PDO fetch mode.
     * @return void
     * @throws EhrlichAndreas_Db_Adapter_Exception
     */
    public function setFetchMode ($mode)
    {
        // check for PDO extension
        //if (! extension_loaded('pdo')) {
            /**
             *
             * @see EhrlichAndreas_Db_Exception
             */
        //    throw new EhrlichAndreas_Db_Exception('The PDO extension is required for this adapter but the extension is not loaded');
        //}
        switch ($mode)
        {
            case EhrlichAndreas_Db_Abstract::FETCH_LAZY:
                
            case EhrlichAndreas_Db_Abstract::FETCH_ASSOC:
                
            case EhrlichAndreas_Db_Abstract::FETCH_NUM:
                
            case EhrlichAndreas_Db_Abstract::FETCH_BOTH:
                
            case EhrlichAndreas_Db_Abstract::FETCH_NAMED:
                
            case EhrlichAndreas_Db_Abstract::FETCH_OBJ:
                
                $this->_fetchMode = $mode;

                break;

            default:
                /**
                 *
                 * @see EhrlichAndreas_Db_Exception
                 */
                throw new EhrlichAndreas_Db_Exception("Invalid fetch mode '$mode' specified");

                break;
        }
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
        switch ($type)
        {
            case 'positional':
                
            case 'named':
                
            default:

                return true;
        }
    }

    /**
     * Retrieve server version in PHP style
     *
     * @return string
     */
    public function getServerVersion ()
    {
        $this->_connect();

        try
		{
            $version = $this->_connection->getAttribute(EhrlichAndreas_Db_Abstract::ATTR_SERVER_VERSION);
        }
		catch (Exception $e)
		{
            // In case of the driver doesn't support getting attributes
            return null;
        }

        $matches = array();

        if (preg_match('/((?:[0-9]{1,2}\.){1,3}[0-9]{1,2})/', $version, $matches))
        {
            return $matches[1];
        }
        else
        {
            return null;
        }
    }
}

