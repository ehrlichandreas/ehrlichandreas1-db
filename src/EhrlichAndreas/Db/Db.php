<?php 

//require_once 'EhrlichAndreas/Db/Exception.php';

//require_once 'EhrlichAndreas/Db/Abstract.php';

//require_once 'EhrlichAndreas/Util/Object.php';

/**
 * Class for connecting to SQL databases and performing common operations.
 *
 * @author Ehrlich, Andreas <ehrlich.andreas@googlemail.com>
 */
class EhrlichAndreas_Db_Db extends EhrlichAndreas_Db_Abstract
{

    /**
     * Factory for EhrlichAndreas_Db_Adapter_Abstract classes.
     *
     * First argument may be a string containing the base of the adapter class
     * name, e.g. 'Mysqli' corresponds to class EhrlichAndreas_Db_Adapter_Mysqli. This
     * name is currently case-insensitive, but is not ideal to rely on this
     * behavior.
     * If your class is named 'EhrlichAndreas_Db_Adapter_Pdo_Mysql', where
     * 'EhrlichAndreas_Db_Adapter' is the
     * namespace
     * and 'Pdo_Mysql' is the adapter name, it is best to use the name exactly
     * as it
     * is defined in the class. This will ensure proper use of the factory API.
     *
     * First argument may alternatively be array.
     * The adapter class base name is read from the 'adapter' property.
     * The adapter config parameters are read from the 'params' property.
     *
     * Second argument is optional and may be an associative array of key-value
     * pairs. This is used as the argument to the adapter constructor.
     *
     * If the first argument is of type Iterator, it is assumed to contain
     * all parameters, and the second argument is ignored.
     *
     * @param mixed $adapter
     *            String name of base adapter class, or array.
     * @param mixed $config
     *            OPTIONAL; an array or array with adapter
     *            parameters.
     * @return EhrlichAndreas_Db_Adapter_Abstract
     * @throws EhrlichAndreas_Db_Exception
     */
    public static function factory ($adapter, $config = array())
    {
        if (is_object($config) && method_exists($config, 'toArray'))
        {
            $config = $config->toArray();
        }

        if (is_object($adapter) && method_exists($adapter, 'toArray'))
        {
            $adapter = $adapter->toArray();
        }

        /*
         * Convert array argument to plain string adapter name and separate
         * config object.
         */
        if (is_array($adapter))
        {
            if (isset($adapter['params']))
            {
                $config = $adapter['params'];
            }

            if (empty($config))
            {
                $config = $adapter;
            }

            if (isset($adapter['adapter']))
            {
                $adapter = (string) $adapter['adapter'];
            } else {
                $adapter = null;
            }
        }

        /*
         * Verify that adapter parameters are in an array.
         */
        if (! is_array($config))
        {
            /**
             *
             * @see EhrlichAndreas_Db_Exception
             */
            throw new EhrlichAndreas_Db_Exception('Adapter parameters must be in an array or a Iterator object');
        }

        /*
         * Verify that an adapter name has been specified.
         */
        if (! is_string($adapter) || empty($adapter))
        {
            /**
             *
             * @see EhrlichAndreas_Db_Exception
             */
            throw new EhrlichAndreas_Db_Exception('Adapter name must be specified in a string');
        }

        /*
         * Form full adapter class name
         */
        $adapterNamespace = 'EhrlichAndreas_Db_Adapter';

        if (isset($config['adapterNamespace']))
        {
            if ($config['adapterNamespace'] != '')
            {
                $adapterNamespace = $config['adapterNamespace'];
            }

            unset($config['adapterNamespace']);
        }

        $adapterName = $adapterNamespace . '_';
        
        $adapterName .= str_replace(' ', '_', ucwords(str_replace('_', ' ', strtolower($adapter))));

        /*
         * Load the adapter class. This throws an exception if the specified
         * class cannot be loaded.
         */
        if (! class_exists($adapterName))
        {
            throw new EhrlichAndreas_Db_Exception("Adapter class '$adapterName' does not exist");
        }
        
        $notInstanceOf = true;
        
        $notInstanceOf = $notInstanceOf && ! EhrlichAndreas_Util_Object::isInstanceOf($adapterName, 'EhrlichAndreas_Db_Adapter_Abstract');
        
        $notInstanceOf = $notInstanceOf && ! EhrlichAndreas_Util_Object::isInstanceOf($adapterName, 'Zend_Db_Adapter_Abstract');
        
        $notInstanceOf = $notInstanceOf && ! EhrlichAndreas_Util_Object::isInstanceOf($adapterName, 'MiniPhp_Db_Adapter_Abstract');

        /*
         * Verify that the object created is a descendent of the abstract
         * adapter type.
         */
        if ($notInstanceOf) {
            /**
             *
             * @see EhrlichAndreas_Db_Exception
             */
            throw new EhrlichAndreas_Db_Exception("Adapter class '$adapterName' does not extend EhrlichAndreas_Db_Adapter_Abstract");
        }

        /*
         * Create an instance of the adapter class. Pass the config to the
         * adapter class constructor.
         */
        $adapter = str_replace(' ', '_', ucwords(str_replace('_', ' ', $adapter)));
        
        $config['adapter'] = $adapter;
        
        $dbAdapter = new $adapterName($config);

        return $dbAdapter;
    }

    public static function getAdapterName ($adapter, $config = array())
    {
        if (is_object($config) && method_exists($config, 'toArray'))
        {
            $config = $config->toArray();
        }

        if (is_object($adapter) && method_exists($adapter, 'toArray'))
        {
            $adapter = $adapter->toArray();
        }

        if (is_array($adapter))
        {
            if (isset($adapter['params']))
            {
                $config = $adapter['params'];
            }
            
            if (isset($adapter['adapter']))
            {
                $adapter = (string) $adapter['adapter'];
            }
            else
            {
                $adapter = null;
            }
        }

        if (! is_array($config))
        {
            throw new EhrlichAndreas_Db_Exception('Adapter parameters must be in an array or a Iterator object');
        }

        if (! is_string($adapter) || empty($adapter))
        {
            throw new EhrlichAndreas_Db_Exception('Adapter name must be specified in a string');
        }

        $adapterNamespace = 'EhrlichAndreas_Db_Adapter';
        
        if (isset($config['adapterNamespace']))
        {
            if ($config['adapterNamespace'] != '')
            {
                $adapterNamespace = $config['adapterNamespace'];
            }
            
            unset($config['adapterNamespace']);
        }

        $adapterName = $adapterNamespace . '_';
        
        $adapterName .= str_replace(' ', '_', ucwords(str_replace('_', ' ', strtolower($adapter))));

        return $adapterName;
    }
}

?>