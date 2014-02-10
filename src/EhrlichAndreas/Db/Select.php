<?php 

require_once 'EhrlichAndreas/Db/Exception.php';

require_once 'EhrlichAndreas/Db/Sql.php';

require_once 'EhrlichAndreas/Db/Where.php';

/**
 * Class for SQL SELECT generation and results.
 *
 * @author Ehrlich, Andreas <ehrlich.andreas@googlemail.com>
 */
class EhrlichAndreas_Db_Select extends EhrlichAndreas_Db_Where
{

    /**
     * Bind variables for query
     *
     * @var array
     */
    protected $_bind = array();

    /**
     * The initial values for the $_parts array.
     * NOTE: It is important for the 'FOR_UPDATE' part to be last to ensure
     * maximum compatibility with database adapters.
     *
     * @var array
     */
    protected static $_partsInit = array
    (
        EhrlichAndreas_Db_Sql::DISTINCT     => false,
        EhrlichAndreas_Db_Sql::COLUMNS      => array(),
        EhrlichAndreas_Db_Sql::UNION        => array(),
        EhrlichAndreas_Db_Sql::FROM         => array(),
        EhrlichAndreas_Db_Sql::WHERE        => array(),
        EhrlichAndreas_Db_Sql::GROUP        => array(),
        EhrlichAndreas_Db_Sql::HAVING       => array(),
        EhrlichAndreas_Db_Sql::ORDER        => array(),
        EhrlichAndreas_Db_Sql::LIMIT_COUNT  => null,
        EhrlichAndreas_Db_Sql::LIMIT_OFFSET => null,
        EhrlichAndreas_Db_Sql::FOR_UPDATE   => false,
    );

    /**
     * Specify legal join types.
     *
     * @var array
     */
    protected static $_joinTypes = array
    (
        EhrlichAndreas_Db_Sql::INNER_JOIN,
        EhrlichAndreas_Db_Sql::LEFT_JOIN,
        EhrlichAndreas_Db_Sql::RIGHT_JOIN,
        EhrlichAndreas_Db_Sql::FULL_JOIN,
        EhrlichAndreas_Db_Sql::CROSS_JOIN,
        EhrlichAndreas_Db_Sql::NATURAL_JOIN,
    );

    /**
     * Specify legal union types.
     *
     * @var array
     */
    protected static $_unionTypes = array
    (
        EhrlichAndreas_Db_Sql::SQL_UNION,
        EhrlichAndreas_Db_Sql::SQL_UNION_ALL,
    );

    /**
     * Tracks which columns are being select from each table and join.
     *
     * @var array
     */
    protected $_tableCols = array();

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
     * Get bind variables
     *
     * @return array
     */
    public function getBind ()
    {
        return $this->_bind;
    }

    /**
     * Set bind variables
     *
     * @param mixed $bind
     * @return EhrlichAndreas_Db_Select
     */
    public function bind ($bind)
    {
		//TODO
		$bind = $this->_mapEhrlichAndreasToZend($bind);
		
        $this->_bind = $bind;

        return $this;
    }

    /**
     * Makes the query SELECT DISTINCT.
     *
     * @param bool $flag
     *            Whether or not the SELECT is DISTINCT (default true).
     * @return EhrlichAndreas_Db_Select This EhrlichAndreas_Db_Select object.
     */
    public function distinct ($flag = true)
    {
        $this->_parts[EhrlichAndreas_Db_Sql::DISTINCT] = (bool) $flag;

        return $this;
    }

    /**
     * Adds a FROM table and optional columns to the query.
     *
     * The first parameter $name can be a simple string, in which case the
     * correlation name is generated automatically. If you want to specify
     * the correlation name, the first parameter must be an associative
     * array in which the key is the correlation name, and the value is
     * the physical table name. For example, array('alias' => 'table').
     * The correlation name is prepended to all columns fetched for this
     * table.
     *
     * The second parameter can be a single string or EhrlichAndreas_Db_Expr object,
     * or else an array of strings or EhrlichAndreas_Db_Expr objects.
     *
     * The first parameter can be null or an empty string, in which case
     * no correlation name is generated or prepended to the columns named
     * in the second parameter.
     *
     * @param array|string|EhrlichAndreas_Db_Expr $name
     *            The table name or an associative array
     *            relating correlation name to table name.
     * @param array|string|EhrlichAndreas_Db_Expr $cols
     *            The columns to select from this table.
     * @param string $schema
     *            The schema name to specify, if any.
     * @return EhrlichAndreas_Db_Select This EhrlichAndreas_Db_Select object.
     */
    public function from ($name, $cols = '*', $schema = null)
    {
        return $this->_join(EhrlichAndreas_Db_Sql::FROM, $name, null, $cols, $schema);
    }

    /**
     * Specifies the columns used in the FROM clause.
     *
     * The parameter can be a single string or EhrlichAndreas_Db_Expr object,
     * or else an array of strings or EhrlichAndreas_Db_Expr objects.
     *
     * @param array|string|EhrlichAndreas_Db_Expr $cols
     *            The columns to select from this table.
     * @param string $correlationName
     *            Correlation name of target table. OPTIONAL
     * @return EhrlichAndreas_Db_Select This EhrlichAndreas_Db_Select object.
     */
    public function columns ($cols = '*', $correlationName = null)
    {
        if ($correlationName === null && count($this->_parts[EhrlichAndreas_Db_Sql::FROM])) 
        {
            $correlationNameKeys = array_keys($this->_parts[EhrlichAndreas_Db_Sql::FROM]);
            
            $correlationName = current($correlationNameKeys);
        }

        if (! array_key_exists($correlationName, $this->_parts[EhrlichAndreas_Db_Sql::FROM])) {
            /**
             *
             * @see EhrlichAndreas_Db_Exception
             */
            throw new EhrlichAndreas_Db_Exception("No table has been specified for the FROM clause");
        }

        $this->_tableCols($correlationName, $cols);

        return $this;
    }

    /**
     * Adds a UNION clause to the query.
     *
     * The first parameter has to be an array of EhrlichAndreas_Db_Select or
     * sql query strings.
     *
     * <code>
     * $sql1 = $db->select();
     * $sql2 = "SELECT ...";
     * $select = $db->select()
     * ->union(array($sql1, $sql2))
     * ->order("id");
     * </code>
     *
     * @param array $select
     *            Array of select clauses for the union.
     * @return EhrlichAndreas_Db_Select This EhrlichAndreas_Db_Select object.
     */
    public function union ($select = array(), $type = EhrlichAndreas_Db_Sql::SQL_UNION)
    {
        if (! is_array($select))
        {
            throw new EhrlichAndreas_Db_Exception("union() only accepts an array of EhrlichAndreas_Db_Select instances of sql query strings.");
        }

        if (! in_array($type, self::$_unionTypes))
        {
            throw new EhrlichAndreas_Db_Exception("Invalid union type '{$type}'");
        }
		
		//TODO
		$select = $this->_mapEhrlichAndreasToZend($select);

        foreach ($select as $target)
        {
            $this->_parts[EhrlichAndreas_Db_Sql::UNION][] = array
			(
                $target,
                $type,
            );
        }

        return $this;
    }

    /**
     * Adds a JOIN table and columns to the query.
     *
     * The $name and $cols parameters follow the same logic
     * as described in the from() method.
     *
     * @param array|string|EhrlichAndreas_Db_Expr $name
     *            The table name.
     * @param string $cond
     *            Join on this condition.
     * @param array|string $cols
     *            The columns to select from the joined table.
     * @param string $schema
     *            The database name to specify, if any.
     * @return EhrlichAndreas_Db_Select This EhrlichAndreas_Db_Select object.
     */
    public function join ($name, $cond, $cols = EhrlichAndreas_Db_Sql::SQL_WILDCARD, $schema = null)
    {
        return $this->joinInner($name, $cond, $cols, $schema);
    }

    /**
     * Add an INNER JOIN table and colums to the query
     * Rows in both tables are matched according to the expression
     * in the $cond argument.
     * The result set is comprised
     * of all cases where rows from the left table match
     * rows from the right table.
     *
     * The $name and $cols parameters follow the same logic
     * as described in the from() method.
     *
     * @param array|string|EhrlichAndreas_Db_Expr $name
     *            The table name.
     * @param string $cond
     *            Join on this condition.
     * @param array|string $cols
     *            The columns to select from the joined table.
     * @param string $schema
     *            The database name to specify, if any.
     * @return EhrlichAndreas_Db_Select This EhrlichAndreas_Db_Select object.
     */
    public function joinInner ($name, $cond, $cols = EhrlichAndreas_Db_Sql::SQL_WILDCARD, $schema = null)
    {
        return $this->_join(EhrlichAndreas_Db_Sql::INNER_JOIN, $name, $cond, $cols, $schema);
    }

    /**
     * Add a LEFT OUTER JOIN table and colums to the query
     * All rows from the left operand table are included,
     * matching rows from the right operand table included,
     * and the columns from the right operand table are filled
     * with NULLs if no row exists matching the left table.
     *
     * The $name and $cols parameters follow the same logic
     * as described in the from() method.
     *
     * @param array|string|EhrlichAndreas_Db_Expr $name
     *            The table name.
     * @param string $cond
     *            Join on this condition.
     * @param array|string $cols
     *            The columns to select from the joined table.
     * @param string $schema
     *            The database name to specify, if any.
     * @return EhrlichAndreas_Db_Select This EhrlichAndreas_Db_Select object.
     */
    public function joinLeft ($name, $cond, $cols = EhrlichAndreas_Db_Sql::SQL_WILDCARD, $schema = null)
    {
        return $this->_join(EhrlichAndreas_Db_Sql::LEFT_JOIN, $name, $cond, $cols, $schema);
    }

    /**
     * Add a RIGHT OUTER JOIN table and colums to the query.
     * Right outer join is the complement of left outer join.
     * All rows from the right operand table are included,
     * matching rows from the left operand table included,
     * and the columns from the left operand table are filled
     * with NULLs if no row exists matching the right table.
     *
     * The $name and $cols parameters follow the same logic
     * as described in the from() method.
     *
     * @param array|string|EhrlichAndreas_Db_Expr $name
     *            The table name.
     * @param string $cond
     *            Join on this condition.
     * @param array|string $cols
     *            The columns to select from the joined table.
     * @param string $schema
     *            The database name to specify, if any.
     * @return EhrlichAndreas_Db_Select This EhrlichAndreas_Db_Select object.
     */
    public function joinRight ($name, $cond, $cols = EhrlichAndreas_Db_Sql::SQL_WILDCARD, $schema = null)
    {
        return $this->_join(EhrlichAndreas_Db_Sql::RIGHT_JOIN, $name, $cond, $cols, $schema);
    }

    /**
     * Add a FULL OUTER JOIN table and colums to the query.
     * A full outer join is like combining a left outer join
     * and a right outer join. All rows from both tables are
     * included, paired with each other on the same row of the
     * result set if they satisfy the join condition, and otherwise
     * paired with NULLs in place of columns from the other table.
     *
     * The $name and $cols parameters follow the same logic
     * as described in the from() method.
     *
     * @param array|string|EhrlichAndreas_Db_Expr $name
     *            The table name.
     * @param string $cond
     *            Join on this condition.
     * @param array|string $cols
     *            The columns to select from the joined table.
     * @param string $schema
     *            The database name to specify, if any.
     * @return EhrlichAndreas_Db_Select This EhrlichAndreas_Db_Select object.
     */
    public function joinFull ($name, $cond, $cols = EhrlichAndreas_Db_Sql::SQL_WILDCARD, $schema = null)
    {
        return $this->_join(EhrlichAndreas_Db_Sql::FULL_JOIN, $name, $cond, $cols, $schema);
    }

    /**
     * Add a CROSS JOIN table and colums to the query.
     * A cross join is a cartesian product; there is no join condition.
     *
     * The $name and $cols parameters follow the same logic
     * as described in the from() method.
     *
     * @param array|string|EhrlichAndreas_Db_Expr $name
     *            The table name.
     * @param array|string $cols
     *            The columns to select from the joined table.
     * @param string $schema
     *            The database name to specify, if any.
     * @return EhrlichAndreas_Db_Select This EhrlichAndreas_Db_Select object.
     */
    public function joinCross ($name, $cols = EhrlichAndreas_Db_Sql::SQL_WILDCARD, $schema = null)
    {
        return $this->_join(EhrlichAndreas_Db_Sql::CROSS_JOIN, $name, null, $cols, $schema);
    }

    /**
     * Add a NATURAL JOIN table and colums to the query.
     * A natural join assumes an equi-join across any column(s)
     * that appear with the same name in both tables.
     * Only natural inner joins are supported by this API,
     * even though SQL permits natural outer joins as well.
     *
     * The $name and $cols parameters follow the same logic
     * as described in the from() method.
     *
     * @param array|string|EhrlichAndreas_Db_Expr $name
     *            The table name.
     * @param array|string $cols
     *            The columns to select from the joined table.
     * @param string $schema
     *            The database name to specify, if any.
     * @return EhrlichAndreas_Db_Select This EhrlichAndreas_Db_Select object.
     */
    public function joinNatural ($name, $cols = EhrlichAndreas_Db_Sql::SQL_WILDCARD, $schema = null)
    {
        return $this->_join(EhrlichAndreas_Db_Sql::NATURAL_JOIN, $name, null, $cols, $schema);
    }

    /**
     * Adds grouping to the query.
     *
     * @param array|string $spec
     *            The column(s) to group by.
     * @return EhrlichAndreas_Db_Select This EhrlichAndreas_Db_Select object.
     */
    public function group ($spec)
    {
        if (! is_array($spec))
        {
            $spec = array
            (
                $spec,
            );
        }

        foreach ($spec as $val)
        {
            if (preg_match('/\(.*\)/', (string) $val))
            {
                $val = new EhrlichAndreas_Db_Expr($val);
            }
		
			//TODO
			$val = $this->_mapEhrlichAndreasToZend($val);
		

            $this->_parts[EhrlichAndreas_Db_Sql::GROUP][] = $val;
        }

        return $this;
    }

    /**
     * Adds a HAVING condition to the query by AND.
     *
     * If a value is passed as the second param, it will be quoted
     * and replaced into the condition wherever a question-mark
     * appears. See {@link where()} for an example
     *
     * @param string $cond
     *            The HAVING condition.
     * @param mixed $value
     *            OPTIONAL The value to quote into the condition.
     * @param int $type
     *            OPTIONAL The type of the given value
     * @return EhrlichAndreas_Db_Select This EhrlichAndreas_Db_Select object.
     */
    public function having ($cond, $value = null, $type = null)
    {
		//TODO
		$cond = $this->_mapEhrlichAndreasToZend($cond);
        
		$value = $this->_mapEhrlichAndreasToZend($value);
		
        if ($value !== null)
        {
            $cond = $this->_adapter->quoteInto($cond, $value, $type);
        }

        if ($this->_parts[EhrlichAndreas_Db_Sql::HAVING]) {
            $this->_parts[EhrlichAndreas_Db_Sql::HAVING][] = EhrlichAndreas_Db_Sql::SQL_AND . " ($cond)";
        } else {
            $this->_parts[EhrlichAndreas_Db_Sql::HAVING][] = "($cond)";
        }

        return $this;
    }

    /**
     * Adds a HAVING condition to the query by OR.
     *
     * Otherwise identical to orHaving().
     *
     * @param string $cond
     *            The HAVING condition.
     * @param mixed $value
     *            OPTIONAL The value to quote into the condition.
     * @param int $type
     *            OPTIONAL The type of the given value
     * @return EhrlichAndreas_Db_Select This EhrlichAndreas_Db_Select object.
     *
     * @see having()
     */
    public function orHaving ($cond, $value = null, $type = null)
    {
		//TODO
		$cond = $this->_mapEhrlichAndreasToZend($cond);
        
		$value = $this->_mapEhrlichAndreasToZend($value);
		
        if ($value !== null)
        {
            $cond = $this->_adapter->quoteInto($cond, $value, $type);
        }

        if ($this->_parts[EhrlichAndreas_Db_Sql::HAVING]) {
            $this->_parts[EhrlichAndreas_Db_Sql::HAVING][] = EhrlichAndreas_Db_Sql::SQL_OR . " ($cond)";
        } else {
            $this->_parts[EhrlichAndreas_Db_Sql::HAVING][] = "($cond)";
        }

        return $this;
    }

    /**
     * Adds a row order to the query.
     *
     * @param mixed $spec
     *            The column(s) and direction to order by.
     * @return EhrlichAndreas_Db_Select This EhrlichAndreas_Db_Select object.
     */
    public function order ($spec)
    {
        if (! is_array($spec))
        {
            $spec = array
            (
                $spec,
            );
        }

        // force 'ASC' or 'DESC' on each order spec, default is ASC.
        foreach ($spec as $val)
        {
            if (is_object($val) && method_exists($val, '__toString') && !method_exists($val, 'assemble'))
            {
                $expr = $val->__toString();

                if (empty($expr))
                {
                    continue;
                }
		
				//TODO
				$val = $this->_mapEhrlichAndreasToZend($val);

                $this->_parts[EhrlichAndreas_Db_Sql::ORDER][] = $val;
            }
            else
            {
                if (empty($val))
                {
                    continue;
                }

                $direction = EhrlichAndreas_Db_Sql::SQL_ASC;

                if (preg_match('/(.*\W)(' . EhrlichAndreas_Db_Sql::SQL_ASC . '|' . EhrlichAndreas_Db_Sql::SQL_DESC . ')\b/si', $val, $matches))
                {
                    $val = trim($matches[1]);
                    
                    $direction = $matches[2];
                }

                if (preg_match('/\(.*\)/', $val))
                {
                    $val = new EhrlichAndreas_Db_Expr($val);
                }
		
				//TODO
				$val = $this->_mapEhrlichAndreasToZend($val);

                $this->_parts[EhrlichAndreas_Db_Sql::ORDER][] = array
                (
                    $val,
                    $direction,
                );
            }
        }

        return $this;
    }

    /**
     * Sets a limit count and offset to the query.
     *
     * @param int $count
     *            OPTIONAL The number of rows to return.
     * @param int $offset
     *            OPTIONAL Start returning after this many rows.
     * @return EhrlichAndreas_Db_Select This EhrlichAndreas_Db_Select object.
     */
    public function limit ($count = null, $offset = null)
    {
        $this->_parts[EhrlichAndreas_Db_Sql::LIMIT_COUNT] = (int) $count;
        
        $this->_parts[EhrlichAndreas_Db_Sql::LIMIT_OFFSET] = (int) $offset;

        return $this;
    }

    /**
     * Sets the limit and count by page number.
     *
     * @param int $page
     *            Limit results to this page number.
     * @param int $rowCount
     *            Use this many rows per page.
     * @return EhrlichAndreas_Db_Select This EhrlichAndreas_Db_Select object.
     */
    public function limitPage ($page, $rowCount)
    {
        $page = ($page > 0) ? $page : 1;
        
        $rowCount = ($rowCount > 0) ? $rowCount : 1;

        $this->_parts[EhrlichAndreas_Db_Sql::LIMIT_COUNT] = (int) $rowCount;
        
        $this->_parts[EhrlichAndreas_Db_Sql::LIMIT_OFFSET] = (int) $rowCount * ($page - 1);

        return $this;
    }

    /**
     * Makes the query SELECT FOR UPDATE.
     *
     * @param bool $flag
     *            Whether or not the SELECT is FOR UPDATE (default true).
     * @return EhrlichAndreas_Db_Select This EhrlichAndreas_Db_Select object.
     */
    public function forUpdate ($flag = true)
    {
        $this->_parts[EhrlichAndreas_Db_Sql::FOR_UPDATE] = (bool) $flag;

        return $this;
    }

    /**
     * Converts this object to an SQL SELECT string.
     *
     * @return string null object as a SELECT string. (or null if a string
     *         cannot be produced.)
     */
    public function assemble ()
    {
        $sql = EhrlichAndreas_Db_Sql::SQL_SELECT;

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
     * @return EhrlichAndreas_Db_Select
     */
    public function reset ($part = null)
    {
        parent::reset($part);

        return $this;
    }

    /**
     * Gets the EhrlichAndreas_Db_Adapter_Abstract for this
     * particular EhrlichAndreas_Db_Select object.
     *
     * @return EhrlichAndreas_Db_Adapter_Abstract
     */
    public function getAdapter ()
    {
        return $this->_adapter;
    }

    /**
     * Populate the {@link $_parts} 'join' key
     *
     * Does the dirty work of populating the join key.
     *
     * The $name and $cols parameters follow the same logic
     * as described in the from() method.
     *
     * @param null|string $type
     *            Type of join; inner, left, and null are currently supported
     * @param array|string|EhrlichAndreas_Db_Expr|Zend_Db_Expr $name
     *            Table name
     * @param string $cond
     *            Join on this condition
     * @param array|string $cols
     *            The columns to select from the joined table
     * @param string $schema
     *            The database name to specify, if any.
     * @return EhrlichAndreas_Db_Select This EhrlichAndreas_Db_Select object
     * @throws EhrlichAndreas_Db_Exception
     */
    protected function _join ($type, $name, $cond, $cols, $schema = null)
    {
        if (! in_array($type, self::$_joinTypes) && $type != EhrlichAndreas_Db_Sql::FROM) {
            /**
             *
             * @see EhrlichAndreas_Db_Exception
             */
            throw new EhrlichAndreas_Db_Exception("Invalid join type '$type'");
        }

        if (count($this->_parts[EhrlichAndreas_Db_Sql::UNION]))
        {
            throw new EhrlichAndreas_Db_Exception("Invalid use of table with " . EhrlichAndreas_Db_Sql::SQL_UNION);
        }
		
		//TODO
		$name = $this->_mapEhrlichAndreasToZend($name);
        
		$cond = $this->_mapEhrlichAndreasToZend($cond);
        
		$cols = $this->_mapEhrlichAndreasToZend($cols);
        
		$schema = $this->_mapEhrlichAndreasToZend($schema);

        if (empty($name))
        {
            $correlationName = $tableName = '';
        }
        elseif (is_array($name))
        {
            // Must be array($correlationName => $tableName) or
            // array($ident, ...)
            foreach ($name as $_correlationName => $_tableName)
            {
                if (is_string($_correlationName))
                {
                    // We assume the key is the correlation name and value
                    // is the table name
                    $tableName = $_tableName;
                    
                    $correlationName = $_correlationName;
                }
                else
                {
                    // We assume just an array of identifiers, with no
                    // correlation name
                    $tableName = $_tableName;
                    
                    $correlationName = $this->_uniqueCorrelation($tableName);
                }
                break;
            }
        }
		elseif (is_object($name) && method_exists($name, '__toString'))
		{
            $tableName = $name;
            
            $correlationName = $this->_uniqueCorrelation('t');
        }
        elseif (preg_match('/^(.+)\s+AS\s+(.+)$/i', $name, $m))
        {
            $tableName = $m[1];
            
            $correlationName = $m[2];
        }
        else
        {
            $tableName = $name;
            
            $correlationName = $this->_uniqueCorrelation($tableName);
        }

        // Schema from table name overrides schema argument
        if (! is_object($tableName) && false !== strpos($tableName, '.'))
        {
            list ($schema, $tableName) = explode('.', $tableName);
        }

        $lastFromCorrelationName = null;

        if (! empty($correlationName))
        {
            if (array_key_exists($correlationName, $this->_parts[EhrlichAndreas_Db_Sql::FROM]))
            {
                /**
                 *
                 * @see EhrlichAndreas_Db_Exception
                 */
                throw new EhrlichAndreas_Db_Exception("You cannot define a correlation name '$correlationName' more than once");
            }

            if ($type == EhrlichAndreas_Db_Sql::FROM)
            {
                // append this from after the last from joinType
                $tmpFromParts = $this->_parts[EhrlichAndreas_Db_Sql::FROM];
                
                $this->_parts[EhrlichAndreas_Db_Sql::FROM] = array();

                // move all the froms onto the stack
                while ($tmpFromParts)
                {
                    $currentCorrelationName = key($tmpFromParts);

                    if ($tmpFromParts[$currentCorrelationName]['joinType'] != EhrlichAndreas_Db_Sql::FROM)
                    {
                        break;
                    }

                    $lastFromCorrelationName = $currentCorrelationName;
                    
                    $this->_parts[EhrlichAndreas_Db_Sql::FROM][$currentCorrelationName] = array_shift($tmpFromParts);
                }
            }
            else
            {
                $tmpFromParts = array();
            }

            $this->_parts[EhrlichAndreas_Db_Sql::FROM][$correlationName] = array
            (
                'joinType'      => $type,
                'schema'        => $schema,
                'tableName'     => $tableName,
                'joinCondition' => $cond,
            );

            while ($tmpFromParts)
            {
                $currentCorrelationName = key($tmpFromParts);
                
                $this->_parts[EhrlichAndreas_Db_Sql::FROM][$currentCorrelationName] = array_shift($tmpFromParts);
            }
        }

        // add to the columns from this joined table
        if ($type == EhrlichAndreas_Db_Sql::FROM && $lastFromCorrelationName == null)
        {
            $lastFromCorrelationName = true;
        }

        $this->_tableCols($correlationName, $cols, $lastFromCorrelationName);

        return $this;
    }

    /**
     * Handle JOIN...
     * USING... syntax
     *
     * This is functionality identical to the existing JOIN methods, however
     * the join condition can be passed as a single column name. This method
     * then completes the ON condition by using the same field for the FROM
     * table and the JOIN table.
     *
     * <code>
     * $select = $db->select()->from('table1')
     * ->joinUsing('table2', 'column1');
     *
     * // SELECT * FROM table1 JOIN table2 ON table1.column1 = table2.column2
     * </code>
     *
     * These joins are called by the developer simply by adding 'Using' to the
     * method name. E.g.
     * * joinUsing
     * * joinInnerUsing
     * * joinFullUsing
     * * joinRightUsing
     * * joinLeftUsing
     *
     * @return EhrlichAndreas_Db_Select This EhrlichAndreas_Db_Select object.
     */
    public function _joinUsing ($type, $name, $cond, $cols = '*', $schema = null)
    {
        if (empty($this->_parts[EhrlichAndreas_Db_Sql::FROM]))
        {
            throw new EhrlichAndreas_Db_Exception("You can only perform a joinUsing after specifying a FROM table");
        }
		
		//TODO
		$name = $this->_mapEhrlichAndreasToZend($name);
        
		$cond = $this->_mapEhrlichAndreasToZend($cond);
        
		$cols = $this->_mapEhrlichAndreasToZend($cols);
        
		$schema = $this->_mapEhrlichAndreasToZend($schema);

        $join = $this->_adapter->quoteIdentifier(key($this->_parts[EhrlichAndreas_Db_Sql::FROM]), true);
        
        $from = $this->_adapter->quoteIdentifier($this->_uniqueCorrelation($name), true);

        $cond1 = $from . '.' . $cond;
        
        $cond2 = $join . '.' . $cond;
        
        $cond = $cond1 . ' = ' . $cond2;

        return $this->_join($type, $name, $cond, $cols, $schema);
    }

    /**
     * Generate a unique correlation name
     *
     * @param string|array $name
     *            A qualified identifier.
     * @return string A unique correlation name.
     */
    private function _uniqueCorrelation ($name)
    {
        if (is_array($name))
        {
            $c = end($name);
        }
        else
        {
            // Extract just the last name of a qualified table name
            $dot = strrpos($name, '.');
            
            $c = ($dot === false) ? $name : substr($name, $dot + 1);
        }

        for ($i = 2; array_key_exists($c, $this->_parts[EhrlichAndreas_Db_Sql::FROM]); ++ $i)
        {
            $c = $name . '_' . (string) $i;
        }

        return $c;
    }

    /**
     * Adds to the internal table-to-column mapping array.
     *
     * @param string $tbl
     *            The table/join the columns come from.
     * @param array|string $cols
     *            The list of columns; preferably as
     *            an array, but possibly as a string containing one column.
     * @param
     *            bool|string True if it should be prepended, a correlation name
     *            if it should be inserted
     * @return void
     */
    protected function _tableCols ($correlationName, $cols, $afterCorrelationName = null)
    {
        if (! is_array($cols))
        {
            $cols = array
            (
                $cols,
            );
        }

        if ($correlationName == null)
        {
            $correlationName = '';
        }

        $columnValues = array();

        foreach (array_filter($cols) as $alias => $col)
        {
            $currentCorrelationName = $correlationName;

            if (is_string($col))
            {
                // Check for a column matching "<column> AS <alias>" and extract
                // the alias name
                if (preg_match('/^(.+)\s+' . EhrlichAndreas_Db_Sql::SQL_AS . '\s+(.+)$/i', $col, $m))
                {
                    $col = $m[1];
                    
                    $alias = $m[2];
                }

                // Check for columns that look like functions and convert to
                // EhrlichAndreas_Db_Expr
                if (preg_match('/\(.*\)/', $col))
                {
                    $col = new EhrlichAndreas_Db_Expr($col);
                }
                elseif (preg_match('/(.+)\.(.+)/', $col, $m))
                {
                    $currentCorrelationName = $m[1];
                    
                    $col = $m[2];
                }
            }

            $columnValues[] = array
            (
                $currentCorrelationName,
                $col,
                is_string($alias) ? $alias : null,
            );
        }
		
		//TODO
		$columnValues = $this->_mapEhrlichAndreasToZend($columnValues);

        if ($columnValues)
        {
            // should we attempt to prepend or insert these values?
            if ($afterCorrelationName === true || is_string($afterCorrelationName))
            {
                $tmpColumns = $this->_parts[EhrlichAndreas_Db_Sql::COLUMNS];
                
                $this->_parts[EhrlichAndreas_Db_Sql::COLUMNS] = array();
            }
            else
            {
                $tmpColumns = array();
            }

            // find the correlation name to insert after
            if (is_string($afterCorrelationName))
            {
                while ($tmpColumns)
                {
                    $this->_parts[EhrlichAndreas_Db_Sql::COLUMNS][] = $currentColumn = array_shift($tmpColumns);
                    
                    if ($currentColumn[0] == $afterCorrelationName)
                    {
                        break;
                    }
                }
            }

            // apply current values to current stack
            foreach ($columnValues as $columnValue)
            {
                array_push($this->_parts[EhrlichAndreas_Db_Sql::COLUMNS], $columnValue);
            }

            // finish ensuring that all previous values are applied (if they
            // exist)
            while ($tmpColumns)
            {
                array_push($this->_parts[EhrlichAndreas_Db_Sql::COLUMNS], array_shift($tmpColumns));
            }
        }
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
        if (count($this->_parts[EhrlichAndreas_Db_Sql::UNION]))
        {
            throw new EhrlichAndreas_Db_Exception("Invalid use of where clause with " . EhrlichAndreas_Db_Sql::SQL_UNION);
        }

        return parent::_where($condition, $value, $type, $bool);
    }

    /**
     *
     * @return array
     */
    protected function _getDummyTable ()
    {
        return array();
    }

    /**
     * Return a quoted schema name
     *
     * @param string $schema
     *            The schema name OPTIONAL
     * @return string null
     */
    protected function _getQuotedSchema ($schema = null)
    {
        if ($schema === null)
        {
            return null;
        }
		
		//TODO
		$schema = $this->_mapEhrlichAndreasToZend($schema);
		
        return $this->_adapter->quoteIdentifier($schema, true) . '.';
    }

    /**
     * Return a quoted table name
     *
     * @param string $tableName
     *            The table name
     * @param string $correlationName
     *            The correlation name OPTIONAL
     * @return string
     */
    protected function _getQuotedTable ($tableName, $correlationName = null)
    {
		//TODO
		$tableName = $this->_mapEhrlichAndreasToZend($tableName);
        
		$correlationName = $this->_mapEhrlichAndreasToZend($correlationName);
		
        return $this->_adapter->quoteTableAs($tableName, $correlationName, true);
    }

    /**
     * Render DISTINCT clause
     *
     * @param string $sql
     *            SQL query
     * @return string
     */
    protected function _renderDistinct ($sql)
    {
        if ($this->_parts[EhrlichAndreas_Db_Sql::DISTINCT])
        {
            $sql .= ' ' . EhrlichAndreas_Db_Sql::SQL_DISTINCT;
        }

        return $sql;
    }

    /**
     * Render DISTINCT clause
     *
     * @param string $sql
     *            SQL query
     * @return string null
     */
    protected function _renderColumns ($sql)
    {
        if (! count($this->_parts[EhrlichAndreas_Db_Sql::COLUMNS]))
        {
            return null;
        }

        $columns = array();

        foreach ($this->_parts[EhrlichAndreas_Db_Sql::COLUMNS] as $columnEntry)
        {
            list ($correlationName, $column, $alias) = $columnEntry;
			
			//TODO
			$correlationName = $this->_mapEhrlichAndreasToZend($correlationName);
            
			$column = $this->_mapEhrlichAndreasToZend($column);
            
			$alias = $this->_mapEhrlichAndreasToZend($alias);

            if (is_object($column) && method_exists($column, '__toString') && !method_exists($column, 'assemble'))
			{
                $columns[] = $this->_adapter->quoteColumnAs($column, $alias, true);
            }
            else
            {
                if ($column == EhrlichAndreas_Db_Sql::SQL_WILDCARD)
                {
                    $column = new EhrlichAndreas_Db_Expr(EhrlichAndreas_Db_Sql::SQL_WILDCARD);
                    $alias = null;
                }
				
				$column = $this->_mapEhrlichAndreasToZend($column);

                if (empty($correlationName))
                {
                    $columns[] = $this->_adapter->quoteColumnAs($column, $alias, true);
                }
                else
                {
                    $columns[] = $this->_adapter->quoteColumnAs(array($correlationName, $column,), $alias, true);
                }
            }
        }

        return $sql .= ' ' . implode(', ', $columns);
    }

    /**
     * Render FROM clause
     *
     * @param string $sql
     *            SQL query
     * @return string
     */
    protected function _renderFrom ($sql)
    {
        /*
         * If no table specified, use RDBMS-dependent solution for table-less
         * query. e.g. DUAL in Oracle.
         */
        if (empty($this->_parts[EhrlichAndreas_Db_Sql::FROM]))
        {
            $this->_parts[EhrlichAndreas_Db_Sql::FROM] = $this->_getDummyTable();
        }

        $from = array();

        foreach ($this->_parts[EhrlichAndreas_Db_Sql::FROM] as $correlationName => $table)
        {
            $tmp = '';

            $joinType = ($table['joinType'] == EhrlichAndreas_Db_Sql::FROM) ? EhrlichAndreas_Db_Sql::INNER_JOIN : $table['joinType'];

            // Add join clause (if applicable)
            if (! empty($from))
            {
                $tmp .= ' ' . strtoupper($joinType) . ' ';
            }

            $tmp .= $this->_getQuotedSchema($table['schema']);
            
            $tmp .= $this->_getQuotedTable($table['tableName'], $correlationName);

            // Add join conditions (if applicable)
            if (! empty($from) && ! empty($table['joinCondition']))
            {
                $tmp .= ' ' . EhrlichAndreas_Db_Sql::SQL_ON . ' ' . $table['joinCondition'];
            }

            // Add the table name and condition add to the list
            $from[] = $tmp;
        }

        // Add the list of all joins
        if (! empty($from))
        {
            $sql .= ' ' . EhrlichAndreas_Db_Sql::SQL_FROM . ' ' . implode("\n", $from);
        }

        return $sql;
    }

    /**
     * Render UNION query
     *
     * @param string $sql
     *            SQL query
     * @return string
     */
    protected function _renderUnion ($sql)
    {
        if ($this->_parts[EhrlichAndreas_Db_Sql::UNION])
        {
            $parts = count($this->_parts[EhrlichAndreas_Db_Sql::UNION]);

            foreach ($this->_parts[EhrlichAndreas_Db_Sql::UNION] as $cnt => $union)
            {
                list ($target, $type) = $union;

                if (is_object($target) && method_exists($target, '__toString') && method_exists($target, 'assemble'))
                {
                    $target = $target->assemble();
                }

                $sql .= $target;

                if ($cnt < $parts - 1)
                {
                    $sql .= ' ' . $type . ' ';
                }
            }
        }

        return $sql;
    }

    /**
     * Render GROUP clause
     *
     * @param string $sql
     *            SQL query
     * @return string
     */
    protected function _renderGroup ($sql)
    {
        if ($this->_parts[EhrlichAndreas_Db_Sql::FROM] && $this->_parts[EhrlichAndreas_Db_Sql::GROUP])
        {
            $group = array();

            foreach ($this->_parts[EhrlichAndreas_Db_Sql::GROUP] as $term)
            {
                $group[] = $this->_adapter->quoteIdentifier($term, true);
            }

            //$sql .= ' ' . EhrlichAndreas_Db_Sql::SQL_GROUP_BY . ' ' . implode(",\n\t", $group);
            
            $sql .= ' ' . EhrlichAndreas_Db_Sql::SQL_GROUP_BY . ' ' . implode(", ", $group);
        }

        return $sql;
    }

    /**
     * Render HAVING clause
     *
     * @param string $sql
     *            SQL query
     * @return string
     */
    protected function _renderHaving ($sql)
    {
        if ($this->_parts[EhrlichAndreas_Db_Sql::FROM] && $this->_parts[EhrlichAndreas_Db_Sql::HAVING])
        {
            $sql .= ' ' . EhrlichAndreas_Db_Sql::SQL_HAVING . ' ' . implode(' ', $this->_parts[EhrlichAndreas_Db_Sql::HAVING]);
        }

        return $sql;
    }

    /**
     * Render ORDER clause
     *
     * @param string $sql
     *            SQL query
     * @return string
     */
    protected function _renderOrder ($sql)
    {
        if ($this->_parts[EhrlichAndreas_Db_Sql::ORDER])
        {
            $order = array();

            foreach ($this->_parts[EhrlichAndreas_Db_Sql::ORDER] as $term)
            {
                if (is_array($term))
                {
                    if (is_numeric($term[0]) && strval(intval($term[0])) == $term[0])
                    {
                        $order[] = (int) trim($term[0]) . ' ' . $term[1];
                    }
                    else
                    {
                        $order[] = $this->_adapter->quoteIdentifier($term[0], true) . ' ' . $term[1];
                    }
                }
                elseif (is_numeric($term) && strval(intval($term)) == $term)
                {
                    $order[] = (int) trim($term);
                }
                else
                {
                    $order[] = $this->_adapter->quoteIdentifier($term, true);
                }
            }

            $sql .= ' ' . EhrlichAndreas_Db_Sql::SQL_ORDER_BY . ' ' . implode(', ', $order);
        }

        return $sql;
    }

    /**
     * Render LIMIT OFFSET clause
     *
     * @param string $sql
     *            SQL query
     * @return string
     */
    protected function _renderLimitoffset ($sql)
    {
        $count = 0;
        
        $offset = 0;

        if (! empty($this->_parts[EhrlichAndreas_Db_Sql::LIMIT_OFFSET]))
        {
            $offset = (int) $this->_parts[EhrlichAndreas_Db_Sql::LIMIT_OFFSET];
            
            $count = PHP_INT_MAX;
        }

        if (! empty($this->_parts[EhrlichAndreas_Db_Sql::LIMIT_COUNT]))
        {
            $count = (int) $this->_parts[EhrlichAndreas_Db_Sql::LIMIT_COUNT];
        }

        /*
         * Add limits clause
         */
        if ($count > 0)
        {
            $sql = trim($this->_adapter->limit($sql, $count, $offset));
        }

        return $sql;
    }

    /**
     * Render FOR UPDATE clause
     *
     * @param string $sql
     *            SQL query
     * @return string
     */
    protected function _renderForupdate ($sql)
    {
        if ($this->_parts[EhrlichAndreas_Db_Sql::FOR_UPDATE])
        {
            $sql .= ' ' . EhrlichAndreas_Db_Sql::SQL_FOR_UPDATE;
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
        if ($this->_parts[EhrlichAndreas_Db_Sql::FROM] && $this->_parts[EhrlichAndreas_Db_Sql::WHERE])
        {
            $sql = parent::_renderWhere($sql);
        }

        return $sql;
    }

    /**
     * Turn magic function calls into non-magic function calls
     * for joinUsing syntax
     *
     * @param string $method
     * @param array $args
     *            OPTIONAL EhrlichAndreas_Db_Table_Select query modifier
     * @return EhrlichAndreas_Db_Select
     * @throws EhrlichAndreas_Db_Exception If an invalid method is called.
     */
    public function __call ($method, array $args)
    {
        $matches = array();

        /**
         * Recognize methods for Has-Many cases:
         * findParent<Class>()
         * findParent<Class>By<Rule>()
         * Use the non-greedy pattern repeat modifier e.g.
         * \w+?
         */
        if (preg_match('/^join([a-zA-Z]*?)Using$/', $method, $matches))
        {
            $type = strtolower($matches[1]);

            if ($type)
            {
                $type .= ' join';

                if (! in_array($type, self::$_joinTypes))
                {
                    throw new EhrlichAndreas_Db_Exception("Unrecognized method '$method()'");
                }

                if (in_array($type, array(EhrlichAndreas_Db_Sql::CROSS_JOIN, EhrlichAndreas_Db_Sql::NATURAL_JOIN, )))
                {
                    throw new EhrlichAndreas_Db_Exception("Cannot perform a joinUsing with method '$method()'");
                }
            }
            else
            {
                $type = EhrlichAndreas_Db_Sql::INNER_JOIN;
            }

            array_unshift($args, $type);

            return call_user_func_array(array($this, '_joinUsing', ), $args);
        }

        throw new EhrlichAndreas_Db_Exception("Unrecognized method '$method()'");
    }
}

?>