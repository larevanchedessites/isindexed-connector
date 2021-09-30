/**
 * @fileoverview Google Data Studio Connector for isindex.com API project urls status.
 * This can retrieve the indexing status of the urls for a given project.
 */

var API_URL = 'https://tool.isindexed.com/api/v1';

var cc = DataStudioApp.createCommunityConnector();
    
function isAdminUser() {
  return true;
}

/**
 * Mandatory function required by Google Data Studio that should
 * return the authentication method required by the connector
 * to authorize the third-party service.
 * https://developers.google.com/datastudio/connector/reference#getauthtype
 * @return {Object} AuthType
 */
function getAuthType() {
  return cc
    .newAuthTypeResponse()
    .setAuthType(cc.AuthType.KEY)
    .setHelpUrl('https://tool.isindexed.com/docapi')
    .build();
}

/**
 * Mandatory function required by Google Data Studio that should
 * determine if the authentication for the third-party service is valid.
 * https://developers.google.com/datastudio/connector/reference#isauthvalid
 * @return {Boolean}
 */
function isAuthValid() {
  var userProperties = PropertiesService.getUserProperties();
  var key = userProperties.getProperty('dscc.key');
  return checkForValidKey(key);
}

/**
 * Mandatory function required by Google Data Studio that should
 * clear user credentials for the third-party service.
 * This function does not accept any arguments and
 * https://developers.google.com/datastudio/connector/reference#resetauth
 * the response is empty.
 */
function resetAuth() {
  var userProperties = PropertiesService.getUserProperties();
  userProperties.deleteProperty('dscc.key');
}

/**
 * Mandatory function required by Google Data Studio that should
 * set the credentials after the user enters either their
 * credential information on the community connector configuration page.
 * https://developers.google.com/datastudio/connector/reference#setcredentials
 * @param {Object} request The set credentials request.
 * @return {object} An object with an errorCode.
 */
function setCredentials(request) {
  var key = request.key;
  var validKey = checkForValidKey(key);
  if (!validKey) {
    return {
      errorCode: 'INVALID_CREDENTIALS'
    };
  }
  var userProperties = PropertiesService.getUserProperties();
  userProperties.setProperty('dscc.key', key);
  return {
    errorCode: 'NONE'
  };
}

/**
 * Mandatory function required by Google Data Studio that should
 * return the user configurable options for the connector.
 * https://developers.google.com/datastudio/connector/reference#getconfig
 * @param {Object} request
 * @return {Object} fields
 */
function getConfig(request) {
  var config = cc.getConfig();

  config
    .newTextInput()
    .setId('projectId')
    .setName(
      'Enter a PROJECT_ID'
    )
    .setHelpText('Enter PROJECT_ID to fetch their urls status. An invalid or blank entry will revert to the default value.')
    .setPlaceholder('123')
    .setAllowOverride(true);
  
  return config.build();
}

/**
 * Supports the getSchema() function
 * @param {Object} request
 * @return {Object} fields
 */
function getFields(request) {
  var fields = cc.getFields();
  var types = cc.FieldType;
  
  fields
    .newDimension()
    .setId('id')
    .setType(types.NUMBER);
  fields.newDimension()
    .setId('url')
    .setType(types.URL);  
  fields.newDimension()
    .setId('title')
    .setType(types.TEXT);
  fields.newDimension()
    .setId('description')
    .setType(types.TEXT);
  fields.newDimension()
    .setId('status')
    .setType(types.NUMBER);
  fields.newDimension()
    .setId('dmca')
    .setType(types.NUMBER);
  fields.newDimension()
    .setId('second_index')
    .setType(types.NUMBER);
  fields.newDimension()
    .setId('created_at')
    .setType(types.YEAR_MONTH_DAY_SECOND);
  fields.newDimension()
    .setId('checked_at')
    .setType(types.YEAR_MONTH_DAY_SECOND);

  return fields;
}

/**
 * Mandatory function required by Google Data Studio that should
 * return the schema for the given request.
 * This provides the information about how the connector's data is organized.
 * https://developers.google.com/datastudio/connector/reference#getschema
 * @param {Object} request
 * @return {Object} fields
 */
function getSchema(request) {
  return {schema: getFields().build()};
}

/**
 * Mandatory function required by Google Data Studio that should
 * return the tabular data for the given request.
 * https://developers.google.com/datastudio/connector/reference#getdata
 * @param {Object} request
 * @return {Object}
 */
function getData(request) {
  var requestedFields = getFields().forIds(
    request.fields.map(function(field) {
      return field.name;
    })
  );
  var userProperties = PropertiesService.getUserProperties();
  var token = userProperties.getProperty('dscc.key');
  var baseURL = API_URL + '/project/' + request.configParams.projectId;
  var options = {
    'method' : 'POST',
    'headers': {
      'Authorization': 'Bearer ' + token,
      'Content-Type': 'application/json'
    },
    'muteHttpExceptions':true
  };
  try {
    var apiResponse = UrlFetchApp.fetch(baseURL, options);
    var parsedResponse = JSON.parse(apiResponse);
    var data = getFormattedData(parsedResponse, requestedFields);
  } catch (e) {
    cc.newUserError()
      .setDebugText('Error fetching data from API. Exception details: ' + e)
      .setText(
        'The connector has encountered an unrecoverable error. Please try again later, or file an issue if this error persists.'
      )
      .throwException();
  }
  
  return {
    schema: requestedFields.build(),
    rows: data
  };
}

/**
 * Formats the parsed response from external data source into correct tabular
 * format and returns only the requestedFields
 *
 * @param {Object} parsedResponse The response string from external data source
 *     parsed into an object in a standard format.
 * @param {Array} requestedFields The fields requested in the getData request.
 * @returns {Array} Array containing rows of data in key-value pairs for each
 *     field.
 */
function getFormattedData(response, requestedFields) {
  var data = [];
  Object.keys(response).map(function(packageName) {
    var urls = response.urls;
    var formattedData = urls.map(function(url) {
      return formatData(requestedFields, url);
    });
    data = data.concat(formattedData);
  });
  
  return data;
}


/**
 * Formats a single row of data into the required format.
 *
 * @param {Object} requestedFields Fields requested in the getData request.
 * @param {Object} url Contains the url data.
 * @returns {Object} Contains values for requested fields in predefined format.
 */
function formatData(requestedFields, url) {
  var row = requestedFields.asArray().map(function(requestedField) {
    switch (requestedField.getId()) {
      case 'id':
        return url.id;
      case 'url':
        return url.url;
      case 'title':
        return url.title;
      case 'description':
        return url.description;
      case 'status':
        return url.status;
      case 'dmca':
        return url.dmca;
      case 'second_index':
        return url.second_index;
      case 'created_at':
        return url.created_at.replace(/[^0-9.]/g, '');
      case 'checked_at':
        return url.checked_at.replace(/[^0-9.]/g, '');
      default:
        return '';
    }
  });
  return {values: row};
}

/**
 * Checks if the Key/Token provided by the user is valid
 * @param {String} key
 * @return {Boolean}
 */
function checkForValidKey(key) {
  var token = key;
  var baseURL = API_URL + '/user/credits';  
  var options = {
    'method' : 'GET',
    'headers': {
      'Authorization': 'Bearer ' + token,
      'Content-Type': 'application/json'
    },
    'muteHttpExceptions':true
  };
  var response = UrlFetchApp.fetch(baseURL, options);
  if (response.getResponseCode() == 200) {
    return true;
  } else {
    return false;
  }
}