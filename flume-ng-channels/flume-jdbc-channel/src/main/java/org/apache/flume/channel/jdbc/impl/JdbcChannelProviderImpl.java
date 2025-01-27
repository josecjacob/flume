/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.channel.jdbc.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.KeyedObjectPoolFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPoolFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.jdbc.ConfigurationConstants;
import org.apache.flume.channel.jdbc.DatabaseType;
import org.apache.flume.channel.jdbc.JdbcChannelException;
import org.apache.flume.channel.jdbc.JdbcChannelProvider;
import org.apache.flume.channel.jdbc.TransactionIsolation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcChannelProviderImpl implements JdbcChannelProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(JdbcChannelProviderImpl.class);


  private static final String EMBEDDED_DERBY_DRIVER_CLASSNAME
        = "org.apache.derby.jdbc.EmbeddedDriver";

  private static final String DEFAULT_DRIVER_CLASSNAME
         = EMBEDDED_DERBY_DRIVER_CLASSNAME;
  private static final String DEFAULT_USERNAME = "sa";
  private static final String DEFAULT_PASSWORD = "";
  private static final String DEFAULT_DBTYPE = "DERBY";

  /** The connection pool. */
  private GenericObjectPool connectionPool;

  /** The statement cache pool */
  private KeyedObjectPoolFactory statementPool;

  /** The data source. */
  private DataSource dataSource;

  /** The database type. */
  private DatabaseType databaseType;

  /** The schema handler. */
  private SchemaHandler schemaHandler;

  /** Transaction factory */
  private JdbcTransactionFactory txFactory;

  /** Connection URL */
  private String connectUrl;

  /** Driver Class Name */
  private String driverClassName;

  @Override
  public void initialize(Context context) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Initializing JDBC Channel provider with props: "
          + context);
    }

    initializeDataSource(context);
    initializeSchema(context);
  }

  private void initializeSchema(Context context) {
    String createSchemaFlag = context.getString(
        ConfigurationConstants.CONFIG_CREATE_SCHEMA, "true");

    boolean createSchema = Boolean.valueOf(createSchemaFlag);
    LOGGER.debug("Create schema flag set to: " + createSchema);

    // First check if the schema exists
    schemaHandler = SchemaHandlerFactory.getHandler(databaseType, dataSource);

    if (!schemaHandler.schemaExists()) {
      if (!createSchema) {
        throw new JdbcChannelException("Schema does not exist and "
            + "auto-generation is disabled. Please enable auto-generation of "
            + "schema and try again.");
      }

      // Now create schema
      schemaHandler.createSchemaObjects();
    }

    // Validate all schema objects are as expected
    schemaHandler.validateSchema();
  }


  @Override
  public void close() {
    try {
      connectionPool.close();
    } catch (Exception ex) {
      throw new JdbcChannelException("Unable to close connection pool", ex);
    }

    if (databaseType.equals(DatabaseType.DERBY)
        && driverClassName.equals(EMBEDDED_DERBY_DRIVER_CLASSNAME)) {
      // Need to shut down the embedded Derby instance
      if (connectUrl.startsWith("jdbc:derby:")) {
        int index = connectUrl.indexOf(";");
        String baseUrl = null;
        if (index != -1) {
          baseUrl = connectUrl.substring(0, index+1);
        } else {
          baseUrl = connectUrl + ";";
        }
        String shutDownUrl = baseUrl + "shutdown=true";

        LOGGER.debug("Attempting to shutdown embedded Derby using URL: "
            + shutDownUrl);

        try {
          DriverManager.getConnection(shutDownUrl);
        } catch (SQLException ex) {
          // Shutdown for one db instance is expected to raise SQL STATE 45000
          if (ex.getErrorCode() != 45000) {
            throw new JdbcChannelException(
                "Unable to shutdown embedded Derby: " + shutDownUrl
                + " Error Code: " + ex.getErrorCode(), ex);
          }
          LOGGER.info("Embedded Derby shutdown raised SQL STATE "
              + "45000 as expected.");
        }
      } else {
        LOGGER.warn("Even though embedded Derby drvier was loaded, the connect "
            + "URL is of an unexpected form: " + connectUrl + ". Therfore no "
            + "attempt will be made to shutdown embedded Derby instance.");
      }
    }

    dataSource = null;
    txFactory = null;
    schemaHandler = null;
  }

  @Override
  public void persistEvent(String channel, Event event) {
    PersistableEvent persistableEvent = new PersistableEvent(channel, event);
    JdbcTransactionImpl tx = null;
    try {
      tx = getTransaction();
      tx.begin();

      // Persist the persistableEvent
      schemaHandler.storeEvent(persistableEvent, tx.getConnection());

      tx.commit();
    } catch (Exception ex) {
      tx.rollback();
      throw new JdbcChannelException("Failed to persist event", ex);
    } finally {
      if (tx != null) {
        tx.close();
      }
    }
  }

  @Override
  public Event removeEvent(String channelName) {
    PersistableEvent result = null;
    JdbcTransactionImpl tx = null;
    try {
      tx = getTransaction();
      tx.begin();

      // Retrieve the persistableEvent
      result = schemaHandler.fetchAndDeleteEvent(
          channelName, tx.getConnection());

      tx.commit();
    } catch (Exception ex) {
      tx.rollback();
      throw new JdbcChannelException("Failed to persist event", ex);
    } finally {
      if (tx != null) {
        tx.close();
      }
    }
    return result;
  }

  @Override
  public JdbcTransactionImpl getTransaction() {
    return txFactory.get();
  }

  /**

   * Initializes the datasource and the underlying connection pool.
   * @param properties
   */
  private void initializeDataSource(Context context) {
    driverClassName = context.getString(
        ConfigurationConstants.CONFIG_JDBC_DRIVER_CLASS);

    connectUrl = context.getString(ConfigurationConstants.CONFIG_URL);


    String userName = context.getString(ConfigurationConstants.CONFIG_USERNAME);

    String password = context.getString(ConfigurationConstants.CONFIG_PASSWORD);

    String jdbcPropertiesFile = context.getString(
        ConfigurationConstants.CONFIG_JDBC_PROPERTIES_FILE);

    String dbTypeName = context.getString(
        ConfigurationConstants.CONFIG_DATABASE_TYPE);

    // If connect URL is not specified, use embedded Derby
    if (connectUrl == null || connectUrl.trim().length() == 0) {
      LOGGER.warn("No connection URL specified. "
          + "Using embedded derby database instance.");

      driverClassName = DEFAULT_DRIVER_CLASSNAME;
      userName = DEFAULT_USERNAME;
      password = DEFAULT_PASSWORD;
      dbTypeName = DEFAULT_DBTYPE;

      String homePath = System.getProperty("user.home").replace('\\', '/');

      String defaultDbDir = homePath + "/.flume/jdbc-channel";


      File dbDir = new File(defaultDbDir);
      String canonicalDbDirPath = null;

      try {
        canonicalDbDirPath = dbDir.getCanonicalPath();
      } catch (IOException ex) {
        throw new JdbcChannelException("Unable to find canonical path of dir: "
            + defaultDbDir, ex);
      }

      if (!dbDir.exists()) {
        if (!dbDir.mkdirs()) {
          throw new JdbcChannelException("unable to create directory: "
              + canonicalDbDirPath);
        }
      }

      connectUrl = "jdbc:derby:" + canonicalDbDirPath + "/db;create=true";

      // No jdbc properties file will be used
      jdbcPropertiesFile = null;

      LOGGER.warn("Overriding values for - driver: " + driverClassName
          + ", user: " + userName + "connectUrl: " + connectUrl
          + ", jdbc properties file: " + jdbcPropertiesFile
          + ", dbtype: " + dbTypeName);
    }

    // Right now only Derby and MySQL supported
    databaseType = DatabaseType.getByName(dbTypeName);

    switch (databaseType) {
    case DERBY:
    case MYSQL:
      break;
    default:
      throw new JdbcChannelException("Database " + databaseType
          + " not supported at this time");
    }

    // Register driver
    if (driverClassName == null || driverClassName.trim().length() == 0) {
      throw new JdbcChannelException("No jdbc driver specified");
    }

    try {
      Class.forName(driverClassName);
    } catch (ClassNotFoundException ex) {
      throw new JdbcChannelException("Unable to load driver: "
                  + driverClassName, ex);
    }

    // JDBC Properties
    Properties jdbcProps = new Properties();

    if (jdbcPropertiesFile != null && jdbcPropertiesFile.trim().length() > 0) {
      File jdbcPropsFile = new File(jdbcPropertiesFile.trim());
      if (!jdbcPropsFile.exists()) {
        throw new JdbcChannelException("Jdbc properties file does not exist: "
            + jdbcPropertiesFile);
      }

      InputStream inStream = null;
      try {
        inStream = new FileInputStream(jdbcPropsFile);
        jdbcProps.load(inStream);
      } catch (IOException ex) {
        throw new JdbcChannelException("Unable to load jdbc properties "
            + "from file: " + jdbcPropertiesFile, ex);
      } finally {
        if (inStream != null) {
          try {
            inStream.close();
          } catch (IOException ex) {
            LOGGER.error("Unable to close file: " + jdbcPropertiesFile, ex);
          }
        }
      }
    }

    if (userName != null) {
      Object oldUser = jdbcProps.put("user", userName);
      if (oldUser != null) {
        LOGGER.warn("Overriding user from: " + oldUser + " to: " + userName);
      }
    }

    if (password != null) {
      Object oldPass = jdbcProps.put("password", password);
      if (oldPass != null) {
        LOGGER.warn("Overriding password from the jdbc properties with "
            + " the one specified explicitly.");
      }
    }

    if (LOGGER.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder("JDBC Properties {");
      boolean first = true;
      Enumeration<?> propertyKeys = jdbcProps.propertyNames();
      while (propertyKeys.hasMoreElements()) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        String key = (String) propertyKeys.nextElement();
        sb.append(key).append("=");
        if (key.equalsIgnoreCase("password")) {
          sb.append("*******");
        } else {
          sb.append(jdbcProps.get(key));
        }
      }

      sb.append("}");

      LOGGER.debug(sb.toString());
    }

    // Transaction Isolation
    String txIsolation = context.getString(
        ConfigurationConstants.CONFIG_TX_ISOLATION_LEVEL,
        TransactionIsolation.READ_COMMITTED.getName());

    TransactionIsolation txIsolationLevel =
        TransactionIsolation.getByName(txIsolation);

    LOGGER.debug("Transaction isolation will be set to: " + txIsolationLevel);

    // Setup Datasource
    ConnectionFactory connFactory =
        new DriverManagerConnectionFactory(connectUrl, jdbcProps);

    connectionPool = new GenericObjectPool();

    String maxActiveConnections = context.getString(
        ConfigurationConstants.CONFIG_MAX_CONNECTION, "10");

    int maxActive = 10;
    if (maxActiveConnections != null && maxActiveConnections.length() > 0) {
      try {
        maxActive = Integer.parseInt(maxActiveConnections);
      } catch (NumberFormatException nfe) {
        LOGGER.warn("Max active connections has invalid value: "
                + maxActiveConnections + ", Using default: " + maxActive);
      }
    }

    LOGGER.debug("Max active connections for the pool: " + maxActive);
    connectionPool.setMaxActive(maxActive);

    statementPool = new GenericKeyedObjectPoolFactory(null);

    // Creating the factory instance automatically wires the connection pool
    new PoolableConnectionFactory(connFactory, connectionPool, statementPool,
        databaseType.getValidationQuery(), false, false,
        txIsolationLevel.getCode());

    dataSource = new PoolingDataSource(connectionPool);

    txFactory = new JdbcTransactionFactory(dataSource);
  }
}
