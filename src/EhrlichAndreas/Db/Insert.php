<?php 

//require_once 'EhrlichAndreas/Db/Exception.php';

//require_once 'EhrlichAndreas/Db/Sql.php';

/**
 * Class for SQL INSERT generation and results.
 *
 * @author Ehrlich, Andreas <ehrlich.andreas@googlemail.com>
 */
class EhrlichAndreas_Db_Insert extends EhrlichAndreas_Db_Sql
{

    /**
     * The initial values for the $_parts array.
     *
     * @var array
     */
    protected static $_partsInit = array
    (
        self::INTO      => null,
        self::INSERT    => array(),
    );

    /**
     * Class constructor
     *
     * @param Zend_Db_Adapter_Abstract|EhrlichAndreas_Db_Adapter_Abstract $adapter
     */
    public function __construct ($adapter)
    {
		parent::__construct($adapter);
		
        $this->_parts = self::$_partsInit;
        
        $this->_partsInited = self::$_partsInit;
    }

    /**
     * Render INTO clause
     *
     * @param string   $sql SQL query
     * @return string
     */
    protected function _renderInto($sql)
    {
        if ($this->_parts[self::INTO])
        {
            $sql .= ' ' . self::SQL_INTO . ' ' . $this->_adapter->quoteIdentifier($this->_parts[self::INTO], true);
        }

        return $sql;
    }

    /**
     * Render BIND clause
     *
     * @param string $sql
     *            SQL query
     * @return string
     */
    protected function _renderInsert ($sql)
    {
        $cols = array();
        
        $vals = array();

        foreach ($this->_parts[self::INSERT] as $col => $val)
        {
            $cols[] = $this->_adapter->quoteIdentifier($col, true);

            if (is_object($val) && method_exists($val, '__toString'))
            {
                $vals[] = $val->__toString();
            }
            else
            {
                $vals[] = $this->_adapter->quote($val);
            }

            unset($this->_parts[self::INSERT][$col]);
        }

        $sql = $sql . ' (' . implode(', ', $cols) . ') ' . 'VALUES (' . implode(', ', $vals) . ')';

        return $sql;
    }

    /**
     * Converts this object to an SQL SELECT string.
     *
     * @return string This object as a SELECT string.
     */
    public function assemble ()
    {
        $sql = self::SQL_INSERT;

        foreach (array_keys(self::$_partsInit) as $part)
        {
            $method = '_render' . ucfirst($part);

            if (method_exists($this, $method))
            {
                $sql = $this->$method($sql);
            }
        }

        return $sql;
    }

    /**
     * Adds a INTO table and optional columns to the query.
     *
     * The first parameter $name can be a simple string, in which case the
     * correlation name is generated automatically.
     *
     * @param string|EhrlichAndreas_Db_Expr|Zend_Db_Expr $name
     *            The table name.
     * @return EhrlichAndreas_Db_Insert This EhrlichAndreas_Db_Insert object.
     */
    public function into ($name)
    {
		//TODO
		$name = $this->_mapEhrlichAndreasToZend($name);
		
        $this->_parts[self::INTO] = $name;

        return $this;
    }

    /**
     *
     * @param string|EhrlichAndreas_Db_Expr|Zend_Db_Expr $name
     *            The table key.
     * @param mixed|string|EhrlichAndreas_Db_Expr|Zend_Db_Expr $value
     *            The table value.
     * @return EhrlichAndreas_Db_Insert This EhrlichAndreas_Db_Insert object.
     */
    public function insert ($key, $value)
    {
		//TODO
		$key = $this->_mapEhrlichAndreasToZend($key);
        
		$value = $this->_mapEhrlichAndreasToZend($value);
		
        $this->_parts[self::INSERT][$key] = $value;

        return $this;
    }

    /**
     * Clear parts of the Select object, or an individual part.
     *
     * @param string $part
     *            OPTIONAL
     * @return EhrlichAndreas_Db_Insert
     */
    public function reset ($part = null)
    {
        parent::reset($part);

        return $this;
    }
}

