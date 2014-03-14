<?php 

//require_once 'EhrlichAndreas/Db/Exception.php';

//require_once 'EhrlichAndreas/Db/Sql.php';

/**
 * Class for SQL Where generation.
 *
 * @author Ehrlich, Andreas <ehrlich.andreas@googlemail.com>
 */
class EhrlichAndreas_Db_Where extends EhrlichAndreas_Db_Sql
{

    /**
     * Class constructor
     *
     * @param Zend_Db_Adapter_Abstract|EhrlichAndreas_Db_Adapter_Abstract $adapter
     */
    public function __construct ($adapter)
    {
		parent::__construct($adapter);
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
        if ($this->_parts[EhrlichAndreas_Db_Sql::WHERE])
        {
            $sql .= ' ' . EhrlichAndreas_Db_Sql::SQL_WHERE . ' ' . implode(' ', $this->_parts[EhrlichAndreas_Db_Sql::WHERE]);
        }

        return $sql;
    }

    /**
     * Internal function for creating the where clause
     *
     * @param string $condition
     * @param mixed $value
     *            optional
     * @param string $type
     *            optional
     * @param boolean $bool
     *            true = AND, false = OR
     * @return string clause
     */
    protected function _where ($condition, $value = null, $type = null, $bool = true)
    {
		//TODO
		$condition = $this->_mapEhrlichAndreasToZend($condition);
        
		$value = $this->_mapEhrlichAndreasToZend($value);

        if ($value !== null)
        {
            $condition = $this->_adapter->quoteInto($condition, $value, $type);
        }

        $cond = "";

        if ($this->_parts[EhrlichAndreas_Db_Sql::WHERE])
        {
            if ($bool === true)
            {
                $cond = EhrlichAndreas_Db_Sql::SQL_AND . ' ';
            }
            else
            {
                $cond = EhrlichAndreas_Db_Sql::SQL_OR . ' ';
            }
        }

        return $cond . "($condition)";
    }

    /**
     * Adds a WHERE condition to the query by AND.
     *
     * If a value is passed as the second param, it will be quoted
     * and replaced into the condition wherever a question-mark
     * appears. Array values are quoted and comma-separated.
     *
     * <code>
     * // simplest but non-secure
     * $select->where("id = $id");
     *
     * // secure (ID is quoted but matched anyway)
     * $select->where('id = ?', $id);
     *
     * // alternatively, with named binding
     * $select->where('id = :id');
     * </code>
     *
     * Note that it is more correct to use named bindings in your
     * queries for values other than strings. When you use named
     * bindings, don't forget to pass the values when actually
     * making a query:
     *
     * <code>
     * $db->fetchAll($select, array('id' => 5));
     * </code>
     *
     * @param string $cond
     *            The WHERE condition.
     * @param mixed $value
     *            OPTIONAL The value to quote into the condition.
     * @param int $type
     *            OPTIONAL The type of the given value
     * @return EhrlichAndreas_Db_Where This EhrlichAndreas_Db_Where object.
     */
    public function where ($cond, $value = null, $type = null)
    {
        $this->_parts[EhrlichAndreas_Db_Sql::WHERE][] = $this->_where($cond, $value, $type, true);

        return $this;
    }

    /**
     * Adds a WHERE condition to the query by OR.
     *
     * Otherwise identical to where().
     *
     * @param string $cond
     *            The WHERE condition.
     * @param mixed $value
     *            OPTIONAL The value to quote into the condition.
     * @param int $type
     *            OPTIONAL The type of the given value
     * @return EhrlichAndreas_Db_Where This EhrlichAndreas_Db_Where object.
     *
     * @see where()
     */
    public function orWhere ($cond, $value = null, $type = null)
    {
        $this->_parts[EhrlichAndreas_Db_Sql::WHERE][] = $this->_where($cond, $value, $type, false);

        return $this;
    }
}

