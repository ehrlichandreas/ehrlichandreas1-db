<?php 

require_once 'EhrlichAndreas/Db/Exception.php';

require_once 'EhrlichAndreas/Db/Where.php';

/**
 * Class for SQL SELECT generation and results.
 *
 * @author Ehrlich, Andreas <ehrlich.andreas@googlemail.com>
 */
class EhrlichAndreas_Db_Delete extends EhrlichAndreas_Db_Where
{

    /**
     * The initial values for the $_parts array.
     *
     * @var array
     */
    protected static $_partsInit = array
    (
        self::FROM  => null,
        self::WHERE => array(),
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
     * @param string $sql
     *            SQL query
     * @return string
     */
    protected function _renderFrom ($sql)
    {
        if ($this->_parts[self::FROM])
        {
            $sql .= ' ' . self::SQL_FROM . ' ' . $this->_adapter->quoteIdentifier($this->_parts[self::FROM], true);
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
        $sql = self::SQL_DELETE;

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
     * @return EhrlichAndreas_Db_Delete This EhrlichAndreas_Db_Delete object.
     */
    public function from ($name)
    {
		//TODO
		$name = $this->_mapEhrlichAndreasToZend($name);
		
        $this->_parts[self::FROM] = $name;

        return $this;
    }

    /**
     * Clear parts of the Select object, or an individual part.
     *
     * @param string $part
     *            OPTIONAL
     * @return EhrlichAndreas_Db_Delete
     */
    public function reset ($part = null)
    {
        parent::reset($part);

        return $this;
    }
}

?>