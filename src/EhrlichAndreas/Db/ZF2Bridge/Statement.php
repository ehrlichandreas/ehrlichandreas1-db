<?php 

//require_once 'EhrlichAndreas/Pdo/Statement.php';

//require_once 'EhrlichAndreas/Db/Exception.php';

//require_once 'EhrlichAndreas/Db/Adapter/Pdo/Abstract/Statement.php';

/**
 *
 * @author Ehrlich, Andreas <ehrlich.andreas@googlemail.com>
 */
class EhrlichAndreas_Db_ZF2Bridge_Statement extends EhrlichAndreas_Db_Adapter_Pdo_Abstract_Statement
{
	
	/**
	 * @param EhrlichAndreas_Pdo_Statement $statement
	 */
	public function __construct($statement)
	{
        $this->_stmt = $statement;
    }
    
}

