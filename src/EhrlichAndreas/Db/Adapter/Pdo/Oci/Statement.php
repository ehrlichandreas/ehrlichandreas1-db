<?php 

/**
 * @see EhrlichAndreas_Db_Adapter_Pdo_Abstract_Statement
 */
require_once 'EhrlichAndreas/Db/Adapter/Pdo/Abstract/Statement.php';

/**
 * Proxy class to wrap a PDOStatement object for IBM Databases.
 * Matches the interface of PDOStatement.  All methods simply proxy to the
 * matching method in PDOStatement.  PDOExceptions thrown by PDOStatement
 * are re-thrown as Zend_Db_Statement_Exception.
 *
 * @category   EhrlichAndreas
 * @package    EhrlichAndreas_Db
 * @subpackage Statement
 * @copyright  Copyright (c) 2005-2012 Zend Technologies USA Inc. (http://www.zend.com)
 * @license    http://framework.zend.com/license/new-bsd     New BSD License
 */
class EhrlichAndreas_Db_Adapter_Pdo_Oci_Statement extends EhrlichAndreas_Db_Adapter_Pdo_Abstract_Statement
{

    /**
    * Returns an array containing all of the result set rows.
    *
    * Behaves like parent, but if limit()
    * is used, the final result removes the extra column
    * 'zend_db_rownum'
    *
    * @param int $style OPTIONAL Fetch mode.
    * @param int $col   OPTIONAL Column number, if fetch mode is by column.
    * @return array Collection of rows, each in a format by the fetch mode.
    * @throws EhrlichAndreas_Db_Exception
    */
    public function fetchAll($style = null, $col = null)
    {
        $data = parent::fetchAll($style, $col);
        
        $results = array();
        
        $remove = $this->_adapter->foldCase('zend_db_rownum');

        foreach ($data as $row)
        {
            if (is_array($row) && array_key_exists($remove, $row))
            {
                unset($row[$remove]);
            }
            
            $results[] = $row;
        }
        
        return $results;
    }


    /**
     * Fetches a row from the result set.
     *
     * @param int $style  OPTIONAL Fetch mode for this fetch operation.
     * @param int $cursor OPTIONAL Absolute, relative, or other.
     * @param int $offset OPTIONAL Number for absolute or relative cursors.
     * @return mixed Array, object, or scalar depending on fetch mode.
     * @throws EhrlichAndreas_Db_Exception
     */
    public function fetch($style = null, $cursor = null, $offset = null)
    {
        $row = parent::fetch($style, $cursor, $offset);

        $remove = $this->_adapter->foldCase('zend_db_rownum');
        
        if (is_array($row) && array_key_exists($remove, $row))
        {
            unset($row[$remove]);
        }

        return $row;
    }
}

?>