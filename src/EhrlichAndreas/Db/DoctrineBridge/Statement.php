<?php 

/**
 *
 * @author Ehrlich, Andreas <ehrlich.andreas@googlemail.com>
 */
class EhrlichAndreas_Db_DoctrineBridge_Statement extends EhrlichAndreas_Db_Adapter_Pdo_Abstract_Statement
{
	
	/**
	 * @param EhrlichAndreas_Pdo_Statement $statement
	 */
	public function __construct($statement)
	{
        $this->_stmt = $statement;
    }
    
}

