<?php 

//require_once 'EhrlichAndreas/Db/Exception.php';

//require_once 'EhrlichAndreas/Db/Adapter/Abstract.php';
		
//require_once 'EhrlichAndreas/Db/Expr.php';
        
//require_once 'EhrlichAndreas/Db/Select.php';

//require_once 'EhrlichAndreas/Util/Object.php';

/**
 * Class for SQL generation.
 *
 * @author Ehrlich, Andreas <ehrlich.andreas@googlemail.com>
 */
class EhrlichAndreas_Db_Sql
{

    const DISTINCT = 'distinct';

    const COLUMNS = 'columns';

    const FROM = 'from';

    const UNION = 'union';

    const WHERE = 'where';

    const GROUP = 'group';

    const HAVING = 'having';

    const ORDER = 'order';

    const UPDATE = 'update';

    const SET = 'set';

    const LIMIT_COUNT = 'limitcount';

    const LIMIT_OFFSET = 'limitoffset';

    const FOR_UPDATE = 'forupdate';

    const INNER_JOIN = 'inner join';

    const LEFT_JOIN = 'left join';

    const RIGHT_JOIN = 'right join';

    const FULL_JOIN = 'full join';

    const CROSS_JOIN = 'cross join';

    const NATURAL_JOIN = 'natural join';

    const SQL_WILDCARD = '*';

    const SQL_SELECT = 'SELECT';

    const SQL_UNION = 'UNION';

    const SQL_UNION_ALL = 'UNION ALL';

    const SQL_FROM = 'FROM';

    const SQL_WHERE = 'WHERE';

    const SQL_DISTINCT = 'DISTINCT';

    const SQL_GROUP_BY = 'GROUP BY';

    const SQL_ORDER_BY = 'ORDER BY';

    const SQL_HAVING = 'HAVING';

    const SQL_FOR_UPDATE = 'FOR UPDATE';

    const SQL_AND = 'AND';

    const SQL_AS = 'AS';

    const SQL_OR = 'OR';

    const SQL_ON = 'ON';

    const SQL_ASC = 'ASC';

    const SQL_DESC = 'DESC';

    const SQL_UPDATE = 'UPDATE';

    const SQL_SET = 'SET';

    const INTO = 'into';

    const INSERT = 'insert';

    const SQL_INTO = "INTO";

    const SQL_INSERT = "INSERT";

    const SQL_DELETE     = 'DELETE';

    /**
     * EhrlichAndreas_Db_Adapter_Abstract object.
     *
     * @var EhrlichAndreas_Db_Adapter_Abstract
     */
    protected $_adapter;

    /**
     *
     * @var boolean
     */
    protected $_adapterIsEhrlichAndreas;

    /**
     * The component parts of a sql statement.
     * Initialized to the $_partsInit array in the constructor.
     *
     * @var array
     */
    protected $_parts = array();

    /**
     * The component parts of a sql statement.
     * Initialized to the $_partsInit array in the constructor.
     *
     * @var array
     */
    protected $_partsInited = array();

    /**
     * Class constructor
     *
     * @param Zend_Db_Adapter_Abstract|EhrlichAndreas_Db_Adapter_Abstract $adapter
     */
    public function __construct ($adapter)
    {
		$adapterIsEhrlichAndreas = EhrlichAndreas_Util_Object::isInstanceOf($this->_adapter, 'EhrlichAndreas_Db_Adapter_Abstract');
		
        $this->_adapter = $adapter;
		
        $this->_adapterIsEhrlichAndreas = $adapterIsEhrlichAndreas;
    }

    /**
     * Get part of the structured information for the currect query.
     *
     * @param string $part
     * @return mixed
     * @throws EhrlichAndreas_Db_Exception
     */
    public function getPart ($part)
    {
        $part = strtolower($part);

        if (! array_key_exists($part, $this->_parts))
        {
            throw new EhrlichAndreas_Db_Exception("Invalid Select part '$part'");
        }

        return $this->_parts[$part];
    }

    /**
     * Converts this object to an SQL string.
     *
     * @return string This object as a sql string.
     */
    public function assemble()
	{
        return '';
    }

    /**
     * Converts this object to an SQL string.
     *
     * @return string This object as a sql string.
     */
    public function getSqlString()
	{
        return $this->assemble();
    }

    /**
     * Executes the current select object and returns the result
     *
     * @param integer $fetchMode
     *            OPTIONAL
     * @param mixed $bind
     *            An array of data to bind to the placeholders.
     * @return EhrlichAndreas_Pdo_Statement EhrlichAndreas_Db_Statement
     */
    public function query ($fetchMode = null, $bind = array())
    {
        if (! empty($bind) && method_exists($this, 'bind'))
        {
            $this->bind($bind);
        }

        $stmt = $this->_adapter->query($this);

        if ($fetchMode == null)
        {
            $fetchMode = $this->_adapter->getFetchMode();
        }

        $stmt->setFetchMode($fetchMode);

        return $stmt;
    }

    /**
     * Implements magic method.
     *
     * @return string This object as a sql string.
     */
    public function __toString()
	{
        try
		{
            $sql = $this->assemble();
        }
        catch (Exception $e)
        {
            trigger_error($e->getMessage(),E_USER_WARNING);

            $sql = '';
        }

        return strval($sql);
    }

    /**
     * Clear parts of the Select object, or an individual part.
     *
     * @param string $part
     *            OPTIONAL
     * @return EhrlichAndreas_Db_Sql
     */
    public function reset ($part = null)
    {
        if ($part == null)
        {
            $this->_parts = $this->_partsInited;
        }
        elseif (array_key_exists($part, $this->_partsInited))
        {
            $this->_parts[$part] = $this->_partsInited[$part];
        }

        return $this;
    }
	
	protected function _mapEhrlichAndreasToZend($param)
	{
		if ($this->_adapterIsEhrlichAndreas || (!is_array($param) && !is_object($param)))
		{
			return $param;
		}
		
		if (is_array($param))
		{
			$return = array();
			
			foreach ($param as $key=>$value)
			{
				$key = $this->_mapEhrlichAndreasToZend($key);
				$value = $this->_mapEhrlichAndreasToZend($value);
				
				$return[$key] = $value;
			}
			
			$param = $return;
		}
        
		if (EhrlichAndreas_Util_Object::isInstanceOf($param, 'EhrlichAndreas_Db_Expr'))
		{
			$param = $param->__toString();
			
			$param = new Zend_Db_Expr($param);
		}
		
		if (EhrlichAndreas_Util_Object::isInstanceOf($param, 'EhrlichAndreas_Db_Select'))
		{
			$param = $param->assamble();
			
			$param = new EhrlichAndreas_Zend_Db_Select($param);
		}
		
		return $param;
	}
	
	public static function getQueryAsString($query)
	{
		if (is_object($query) && method_exists($query, 'assemble'))
		{
			$query = $query->assemble();
		}

		if (is_object($query) && method_exists($query, 'getSqlString'))
		{
			$query = $query->getSqlString();
		}

        if (is_object($query) && method_exists($query, '__toString') && ! method_exists($query, 'assemble'))
		{
            $query = $query->__toString();
        }
		
		return $query;
	}
	
	public static function getQueryAsQuotedString($query)
	{
		if (is_object($query) && method_exists($query, 'assemble'))
		{
			$query = $query->assemble();
		}

		if (is_object($query) && method_exists($query, 'getSqlString'))
		{
			$query = $query->getSqlString();
		}

        if (is_object($query) && method_exists($query, '__toString') && ! method_exists($query, 'assemble'))
		{
            $query = '(' . $query->__toString() . ')';
        }
		
		return $query;
	}
}

?>