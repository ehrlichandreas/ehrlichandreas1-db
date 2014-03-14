<?php 

//require_once 'EhrlichAndreas/Db/Exception.php';

//require_once 'EhrlichAndreas/Db/Where.php';

/**
 * Class for SQL UPDATE generation and results.
 *
 * @author Ehrlich, Andreas <ehrlich.andreas@googlemail.com>
 */
class EhrlichAndreas_Db_Update extends EhrlichAndreas_Db_Where
{

    /**
     * The initial values for the $_parts array.
     *
     * @var array
     */
    protected static $_partsInit = array
    (
        EhrlichAndreas_Db_Sql::UPDATE   => null,
        EhrlichAndreas_Db_Sql::SET      => array(),
        EhrlichAndreas_Db_Sql::WHERE    => array(),
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
     *
     * @param string|EhrlichAndreas_Db_Expr $name
     *            The table key.
     * @param mixed|string|EhrlichAndreas_Db_Expr $value
     *            The table value.
     * @return EhrlichAndreas_Db_Update This EhrlichAndreas_Db_Update object.
     */
    public function set ($key, $value)
    {
		//TODO
		$key = $this->_mapEhrlichAndreasToZend($key);
        
		$value = $this->_mapEhrlichAndreasToZend($value);
		
        $this->_parts[EhrlichAndreas_Db_Sql::SET][$key] = $value;
        
        return $this;
    }

    /**
     * Adds a INTO table and optional columns to the query.
     *
     * The first parameter $name can be a simple string, in which case the
     * correlation name is generated automatically.
     *
     * @param string|EhrlichAndreas_Db_Expr $name
     *            The table name.
     * @return EhrlichAndreas_Db_Update This EhrlichAndreas_Db_Update object.
     */
    public function update ($name)
    {
		//TODO
		$name = $this->_mapEhrlichAndreasToZend($name);
		
        $this->_parts[EhrlichAndreas_Db_Sql::UPDATE] = $name;

        return $this;
    }

    /**
     * Render BIND clause
     *
     * @param string $sql
     *            SQL query
     * @return string
     */
    protected function _renderSet ($sql)
    {
        /**
         * Build "col = ?" pairs for the statement,
         * except for EhrlichAndreas_Db_Expr which is treated literally.
         */
        $set = array();

        foreach ($this->_parts[EhrlichAndreas_Db_Sql::SET] as $col => $val)
        {
            if (is_object($val) && method_exists($val, '__toString'))
            {
                $val = $val->__toString();

                unset($this->_parts[EhrlichAndreas_Db_Sql::SET][$col]);
            }
            else
            {
                $val = $this->_adapter->quote($val);
            }

            $set[] = $this->_adapter->quoteIdentifier($col, true) . ' = ' . $val;
        }

        // if ($this->_parts [EhrlichAndreas_Db_Sql::SET]) {
        $sql .= ' ' . EhrlichAndreas_Db_Sql::SQL_SET . ' ' . implode(', ', $set);
        // }

        return $sql;
    }

    /**
     * Render INTO clause
     *
     * @param string $sql
     *            SQL query
     * @return string
     */
    protected function _renderUpdate ($sql)
    {
        if ($this->_parts[EhrlichAndreas_Db_Sql::UPDATE])
        {
            $sql .= ' ' . $this->_adapter->quoteIdentifier($this->_parts[EhrlichAndreas_Db_Sql::UPDATE], true);
        }

        return $sql;
    }

    /**
     * Render WHERE clause
     *
     * @param string $sql
     *            SQL query
     * @return string
     */
    protected function _renderWhere ($sql)
    {
        if ($this->_parts[EhrlichAndreas_Db_Sql::UPDATE] && $this->_parts[EhrlichAndreas_Db_Sql::WHERE])
        {
            $sql = parent::_renderWhere($sql);
        }

        return $sql;
    }

    /**
     * Converts this object to an SQL SELECT string.
     *
     * @return string This object as a SELECT string.
     */
    public function assemble ()
    {
        $sql = EhrlichAndreas_Db_Sql::SQL_UPDATE;

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
     * Clear parts of the Select object, or an individual part.
     *
     * @param string $part
     *            OPTIONAL
     * @return EhrlichAndreas_Db_Update
     */
    public function reset ($part = null)
    {
        parent::reset($part);

        return $this;
    }
}

