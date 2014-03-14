<?php 

//require_once 'EhrlichAndreas/Db/Exception.php';

//require_once 'EhrlichAndreas/Db/Abstract.php';

//require_once 'EhrlichAndreas/Db/Statement/Interface.php';

/**
 * Emulates a PDOStatement for native database adapters.
 *
 * @author Ehrlich, Andreas <ehrlich.andreas@googlemail.com>
 */
abstract class EhrlichAndreas_Db_Statement implements EhrlichAndreas_Db_Statement_Interface
{

    /**
     *
     * @var EhrlichAndreas_Pdo_Statement
     */
    protected $_stmt = null;

    /**
     *
     * @var EhrlichAndreas_Db_Adapter_Abstract
     */
    protected $_adapter = null;

    /**
     * The current fetch mode.
     *
     * @var integer
     */
    protected $_fetchMode = EhrlichAndreas_Db_Abstract::FETCH_ASSOC;

    /**
     * Attributes.
     *
     * @var array
     */
    protected $_attribute = array();

    /**
     * Column result bindings.
     *
     * @var array
     */
    protected $_bindColumn = array();

    /**
     * Query parameter bindings; covers bindParam() and bindValue().
     *
     * @var array
     */
    protected $_bindParam = array();

    /**
     * SQL string split into an array at placeholders.
     *
     * @var array
     */
    protected $_sqlSplit = array();

    /**
     * Parameter placeholders in the SQL string by position in the split array.
     *
     * @var array
     */
    protected $_sqlParam = array();

    /**
     *
     * @var EhrlichAndreas_Db_Profiler_Query
     */
    // protected $_queryId = null;

    /**
     * Constructor for a statement.
     *
     * @param EhrlichAndreas_Db_Adapter_Abstract $adapter
     * @param mixed $sql
     *            Either a string or EhrlichAndreas_Db_Select or Zend_Db_Select.
     */
    public function __construct ($adapter, $sql)
    {
        $this->_adapter = $adapter;

        if (is_object($sql) && method_exists($sql, 'assemble'))
        {
            $sql = $sql->assemble();
        }
        elseif (is_object($sql) && method_exists($sql, '__toString'))
        {
            $sql = $sql->__toString();
        }

        $this->_parseParameters($sql);
        
        $this->_prepare($sql);

        // $this->_queryId = $this->_adapter->getProfiler()->queryStart($sql);
    }

    /**
     * Internal method called by abstract statment constructor to setup
     * the driver level statement
     *
     * @return void
     */
    protected function _prepare ($sql)
    {
        return;
    }

    /**
     *
     * @param string $sql
     * @return void
     */
    protected function _parseParameters ($sql)
    {
        $sql = $this->_stripQuoted($sql);

        // split into text and params
        $this->_sqlSplit = preg_split('/(\?|\:[a-zA-Z0-9_]+)/', $sql, - 1, PREG_SPLIT_DELIM_CAPTURE | PREG_SPLIT_NO_EMPTY);

        // map params
        $this->_sqlParam = array();

        foreach ($this->_sqlSplit as $key => $val)
        {
            if ($val == '?')
            {
                if ($this->_adapter->supportsParameters('positional') === false)
                {
                    /**
                     *
                     * @see EhrlichAndreas_Db_Exception
                     */
                    throw new EhrlichAndreas_Db_Exception("Invalid bind-variable position '$val'");
                }
            }
            elseif ($val[0] == ':')
            {
                if ($this->_adapter->supportsParameters('named') === false)
                {
                    /**
                     *
                     * @see EhrlichAndreas_Db_Exception
                     */
                    throw new EhrlichAndreas_Db_Exception("Invalid bind-variable name '$val'");
                }
            }

            $this->_sqlParam[] = $val;
        }

        // set up for binding
        $this->_bindParam = array();
    }

    /**
     * Remove parts of a SQL string that contain quoted strings
     * of values or identifiers.
     *
     * @param string $sql
     * @return string
     */
    protected function _stripQuoted ($sql)
    {
        // get the character for delimited id quotes,
        // this is usually " but in MySQL is `
        $d = $this->_adapter->quoteIdentifier('a');
        
        $d = $d[0];

        // get the value used as an escaped delimited id quote,
        // e.g. \" or "" or \`
        $de = $this->_adapter->quoteIdentifier($d);
        
        $de = substr($de, 1, 2);
        
        $de = str_replace('\\', '\\\\', $de);

        // get the character for value quoting
        // this should be '
        $q = $this->_adapter->quote('a');
        
        $q = $q[0];

        // get the value used as an escaped quote,
        // e.g. \' or ''
        $qe = $this->_adapter->quote($q);
        
        $qe = substr($qe, 1, 2);
        
        $qe = str_replace('\\', '\\\\', $qe);

        // get a version of the SQL statement with all quoted
        // values and delimited identifiers stripped out
        // remove "foo\"bar"
        $sql = preg_replace("/$q($qe|\\\\{2}|[^$q])*$q/", '', $sql);
        
        // remove 'foo\'bar'
        if (! empty($q))
        {
            $sql = preg_replace("/$q($qe|[^$q])*$q/", '', $sql);
        }

        return $sql;
    }

    /**
     * Bind a column of the statement result set to a PHP variable.
     *
     * @param string $column
     *            Name the column in the result set, either by
     *            position or by name.
     * @param mixed $param
     *            Reference to the PHP variable containing the value.
     * @param mixed $type
     *            OPTIONAL
     * @return bool
     */
    public function bindColumn ($column, &$param, $type = null)
    {
        $this->_bindColumn[$column] = & $param;

        return true;
    }

    /**
     * Binds a parameter to the specified variable name.
     *
     * @param mixed $parameter
     *            Name the parameter, either integer or string.
     * @param mixed $variable
     *            Reference to PHP variable containing the value.
     * @param mixed $type
     *            OPTIONAL Datatype of SQL parameter.
     * @param mixed $length
     *            OPTIONAL Length of SQL parameter.
     * @param mixed $options
     *            OPTIONAL Other options.
     * @return bool
     */
    public function bindParam ($parameter, &$variable, $type = null, $length = null, $options = null)
    {
        if (! is_int($parameter) && ! is_string($parameter))
        {
            /**
             *
             * @see EhrlichAndreas_Db_Exception
             */
            throw new EhrlichAndreas_Db_Exception('Invalid bind-variable position');
        }

        $position = null;

        if (($intval = (int) $parameter) > 0 && $this->_adapter->supportsParameters('positional'))
        {
            if ($intval >= 1 || $intval <= count($this->_sqlParam))
            {
                $position = $intval;
            }
        }
        else
        {
            if ($this->_adapter->supportsParameters('named'))
            {
                if ($parameter[0] != ':')
                {
                    $parameter = ':' . $parameter;
                }

                if (in_array($parameter, $this->_sqlParam) !== false)
                {
                    $position = $parameter;
                }
            }
        }

        if ($position === null)
        {
            /**
             *
             * @see EhrlichAndreas_Db_Exception
             */
            throw new EhrlichAndreas_Db_Exception("Invalid bind-variable position '$parameter'");
        }

        // Finally we are assured that $position is valid
        $this->_bindParam[$position] = & $variable;

        return $this->_bindParam($position, $variable, $type, $length, $options);
    }

    /**
     * Binds a value to a parameter.
     *
     * @param mixed $parameter
     *            Name the parameter, either integer or string.
     * @param mixed $value
     *            Scalar value to bind to the parameter.
     * @param mixed $type
     *            OPTIONAL Datatype of the parameter.
     * @return bool
     */
    public function bindValue ($parameter, $value, $type = null)
    {
        return $this->bindParam($parameter, $value, $type);
    }

    /**
     * Executes a prepared statement.
     *
     * @param array $params
     *            OPTIONAL Values to bind to parameter placeholders.
     * @return bool
     */
    public function execute (array $params = null)
    {
        return $this->_execute($params);

        /*
         * Simple case - no query profiler to manage.
         */
        /*if ($this->_queryId === null) {
            return $this->_execute($params);
        }*/

        /*
         * Do the same thing, but with query profiler management before and
         * after the execute.
         */
        /*$prof = $this->_adapter->getProfiler();
        $qp = $prof->getQueryProfile($this->_queryId);

        if ($qp->hasEnded()) {
            $this->_queryId = $prof->queryClone($qp);
            $qp = $prof->getQueryProfile($this->_queryId);
        }

        if ($params !== null) {
            $qp->bindParams($params);
        } else {
            $qp->bindParams($this->_bindParam);
        }

        $qp->start($this->_queryId);

        $retval = $this->_execute($params);

        $prof->queryEnd($this->_queryId);

        return $retval;*/
    }

    /**
     * Returns an array containing all of the result set rows.
     *
     * @param int $style
     *            OPTIONAL Fetch mode.
     * @param int $col
     *            OPTIONAL Column number, if fetch mode is by column.
     * @return array Collection of rows, each in a format by the fetch mode.
     */
    public function fetchAll ($style = null, $col = null)
    {
        $data = array();

        if ($style === EhrlichAndreas_Db_Abstract::FETCH_COLUMN && $col === null)
        {
            $col = 0;
        }

        if ($col === null)
        {
            do
            {
                $row = $this->fetch($style);

                if ($row)
                {
                    $data[] = $row;
                }
            }
            while ($row);
        }
        else
        {
            do
            {
                $val = $this->fetchColumn($col);

                if ($val !== false)
				{
                    $data[] = $val;
                }
            }
			while ($val !== false);
        }
        return $data;
    }

    /**
     * Returns a single column from the next row of a result set.
     *
     * @param int $col
     *            OPTIONAL Position of the column to fetch.
     * @return string One value from the next row of result set, or false.
     */
    public function fetchColumn ($col = 0)
    {
        $col = (int) $col;
        
        $row = $this->fetch(EhrlichAndreas_Db_Abstract::FETCH_NUM);

        if (! is_array($row))
        {
            return false;
        }

        return $row[$col];
    }

    /**
     * Fetches the next row and returns it as an object.
     *
     * @param string $class
     *            OPTIONAL Name of the class to create.
     * @param array $config
     *            OPTIONAL Constructor arguments for the class.
     * @return mixed One object instance of the specified class, or false.
     */
    public function fetchObject ($class = 'stdClass', array $config = array())
    {
        $obj = new $class($config);
        
        $row = $this->fetch(EhrlichAndreas_Db_Abstract::FETCH_ASSOC);

        if (! is_array($row))
        {
            return false;
        }

        foreach ($row as $key => $val)
        {
            $obj->$key = $val;
        }

        return $obj;
    }

    /**
     * Retrieve a statement attribute.
     *
     * @param string $key
     *            Attribute name.
     * @return mixed Attribute value.
     */
    public function getAttribute ($key)
    {
        if (array_key_exists($key, $this->_attribute))
        {
            return $this->_attribute[$key];
        }
    }

    /**
     * Set a statement attribute.
     *
     * @param string $key
     *            Attribute name.
     * @param mixed $val
     *            Attribute value.
     * @return bool
     */
    public function setAttribute ($key, $val)
    {
        $this->_attribute[$key] = $val;
    }

    /**
     * Set the default fetch mode for this statement.
     *
     * @param int $mode
     *            The fetch mode.
     * @return bool
     * @throws EhrlichAndreas_Db_Exception
     */
    public function setFetchMode ($mode)
    {
        switch ($mode)
        {
            case EhrlichAndreas_Db_Abstract::FETCH_NUM:
                
            case EhrlichAndreas_Db_Abstract::FETCH_ASSOC:
                
            case EhrlichAndreas_Db_Abstract::FETCH_BOTH:
                
            case EhrlichAndreas_Db_Abstract::FETCH_OBJ:
                
                $this->_fetchMode = $mode;
                
                break;
            
            case EhrlichAndreas_Db_Abstract::FETCH_BOUND:
                
            default:
                
                $this->closeCursor();
                /**
                 *
                 * @see EhrlichAndreas_Db_Exception
                 */
                throw new EhrlichAndreas_Db_Exception('invalid fetch mode');
                
                break;
        }
    }

    /**
     * Helper function to map retrieved row
     * to bound column variables
     *
     * @param array $row
     * @return bool True
     */
    public function _fetchBound ($row)
    {
        foreach ($row as $key => $value)
        {
            // bindColumn() takes 1-based integer positions
            // but fetch() returns 0-based integer indexes
            if (is_int($key))
            {
                $key ++;
            }

            // set results only to variables that were bound previously
            if (isset($this->_bindColumn[$key]))
            {
                $this->_bindColumn[$key] = $value;
            }
        }
        
        return true;
    }

    /**
     * Gets the EhrlichAndreas_Db_Adapter_Abstract for this
     * particular EhrlichAndreas_Db_Statement object.
     *
     * @return EhrlichAndreas_Db_Adapter_Abstract
     */
    public function getAdapter ()
    {
        return $this->_adapter;
    }

    /**
     * Gets the resource or object setup by the
     * _parse
     *
     * @return unknown_type
     */
    public function getDriverStatement ()
    {
        return $this->_stmt;
    }
}

