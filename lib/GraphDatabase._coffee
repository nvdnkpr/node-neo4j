# TODO many of these functions take a callback but, in some cases, call the
# callback immediately (e.g. if a value is cached). we should probably make
# sure to always call callbacks asynchronously, to prevent race conditions.
# this can be done in Streamline syntax by adding one line before cases where
# we're returning immediately: process.nextTick _

# TODO document async conventions and Streamline futures for all methods!

status = require 'http-status'

util = require './util'
adjustError = util.adjustError

Relationship = require './Relationship'
Node = require './Node'

#
# The class corresponding to a Neo4j graph database. Start here.
#
module.exports = class GraphDatabase

    #
    # Construct a new client for the Neo4j graph database available at the
    # given (root) URL.
    #
    # @param url {String} The (root) URL that the Neo4j graph database is
    #   available at, e.g. `http://localhost:7474/`. This URL should include
    #   HTTP Basic Authentication info if needed, e.g.
    #   `http://user:password@example.com/`.
    #
    constructor: (url) ->
        @url = url
        @_request = util.wrapRequestForAuth url

        # Cache
        @_root = null
        @_services = null

    ### Database: ###

    #
    # Purge this client's cache of API endpoints for this graph database.
    #
    # @private
    #
    _purgeCache: ->
        @_root = null
        @_services = null

    #
    # Fetch, cache, and "return" (via callback) the API root data for this
    # graph database.
    #
    # @private
    # @param callback {Function}
    # @return {Object}
    #
    _getRoot: (_) ->
        if @_root?
            return @_root

        try
            response = @_request.get @url, _

            if response.statusCode isnt status.OK
                throw response

            @_root = JSON.parse response.body
            return @_root

        catch error
            throw adjustError error

    #
    # Fetch, cache, and "return" (via callback) the API services data for this
    # graph database.
    #
    # @private
    # @param callback {Function}
    # @return {Object}
    #
    getServices: (_) ->
        if @_services?
            return @_services

        try
            root = @_getRoot _
            response = @_request.get root.data, _

            if response.statusCode isnt status.OK
                throw response

            @_services = JSON.parse response.body
            return @_services

        catch error
            throw adjustError error

    #
    # Fetch and "return" (via callback) the Neo4j version as a float.
    #
    # @note This doesn't preserve "milestone" information, e.g. "M06".
    #
    # @param callback {Function}
    # @return {Number}
    #
    getVersion: (_) ->
        try
            services = @getServices _

            # Neo4j 1.5 onwards report their version number here;
            # if it's not there, assume Neo4j 1.4.
            parseFloat services['neo4j_version'] or '1.4'

        catch error
            throw adjustError

    ### Nodes: ###

    #
    # Create and immediately return a new, unsaved node with the given
    # properties.
    #
    # @note This node will *not* be persisted to the database until and unless
    #   its {Node#save save()} method is called.
    #
    # @param data {Object} The properties this new node should have.
    # @return {Node}
    #
    createNode: (data) ->
        data = data || {}
        node = new Node this,
            data: data
        return node

    #
    # Fetch and "return" (via callback) the node at the given URL.
    # Throws an error if no node exists at this URL.
    #
    # @todo Should this indeed throw an error if no node exists at this URL?
    #   Or should we be returning undefined?
    #
    # @param url {String}
    # @param callback {Function}
    # @return {Node}
    #
    getNode: (url, _) ->
        try
            response = @_request.get url, _

            if response.statusCode isnt status.OK

                # Node not found
                if response.statusCode is status.NOT_FOUND
                    throw new Error "No node at #{url}"

                throw response

            node = new Node this, JSON.parse response.body
            return node

        catch error
            throw adjustError error

    #
    # Fetch and "return" (via callback) the node indexed under the given
    # property and value in the given index. If none exists, returns
    # undefined.
    #
    # @note With this method, at most one node is returned. See
    #   {#getIndexedNodes} for returning multiple nodes.
    #
    # @param index {String} The name of the index, e.g. `node_auto_index`.
    # @param property {String} The name of the property, e.g. `username`.
    # @param value {Object} The value of the property, e.g. `aseemk`.
    # @param callback {Function}
    # @return {Node}
    #
    getIndexedNode: (index, property, value, _) ->
        try
            nodes = @getIndexedNodes index, property, value, _

            node = null
            if nodes and nodes.length > 0
                node = nodes[0]
            return node

        catch error
            throw adjustError error

    #
    # Fetch and "return" (via callback) the nodes indexed under the given
    # property and value in the given index. If no such nodes exist, an
    # empty array is returned.
    #
    # @note This method will return multiple nodes if there are multiple hits.
    #   See {#getIndexedNode} for returning at most one node.
    #
    # @param index {String} The name of the index, e.g. `node_auto_index`.
    # @param property {String} The name of the property, e.g. `platform`.
    # @param value {Object} The value of the property, e.g. `xbox`.
    # @param callback {Function}
    # @return {Array<Node>}
    #
    getIndexedNodes: (index, property, value, _) ->
        try
            services = @getServices _

            key = encodeURIComponent property
            val = encodeURIComponent value
            url = "#{services.node_index}/#{index}/#{key}/#{val}"

            response = @_request.get url, _

            if response.statusCode isnt status.OK
                # Database error
                throw response

            # Success
            nodeArray = JSON.parse response.body
            nodes = nodeArray.map (node) =>
                new Node this, node
            return nodes

        catch error
            throw adjustError error

    #
    # Fetch and "return" (via callback) the node with the given Neo4j ID.
    # Throws an error if no node exists with this ID.
    #
    # @todo Should this indeed throw an error if no node exists with this ID?
    #   Or should we be returning undefined?
    #
    # @param id {Number} The integer ID of the node, e.g. `1234`.
    # @param callback {Function}
    # @return {Node}
    #
    getNodeById: (id, _) ->
        try
            services = @getServices _
            url = "#{services.node}/#{id}"
            node = @getNode url, _
            return node

        catch error
            throw adjustError error

    ### Relationships: ###

    createRelationship: (startNode, endNode, type, _) ->
        # TODO: Implement?

    #
    # Fetch and "return" (via callback) the relationship at the given URL.
    # Throws an error if no relationship exists at this URL.
    #
    # @todo Should this indeed throw an error if no relationship exists at
    #   this URL? Or should we be returning undefined?
    #
    # @param url {String}
    # @param callback {Function}
    # @return {Relationship}
    #
    getRelationship: (url, _) ->
        try
            response = @_request.get url, _

            if response.statusCode isnt status.OK
                # TODO: Handle 404
                throw response

            data = JSON.parse response.body

            # Construct relationship
            relationship = new Relationship this, data

            return relationship

        catch error
            throw adjustError error

    #
    # Fetch and "return" (via callback) the relationship indexed under the
    # given property and value in the given index. If none exists, returns
    # undefined.
    #
    # @note With this method, at most one relationship is returned. See
    #   {#getIndexedRelationships} for returning multiple relationships.
    #
    # @param index {String} The name of the index, e.g. `relationship_auto_index`.
    # @param property {String} The name of the property, e.g. `created`.
    # @param value {Object} The value of the property, e.g. `1346713658393`.
    # @param callback {Function}
    # @return {Relationship}
    #
    getIndexedRelationship: (index, property, value, _) ->
        try
            relationships = @getIndexedRelationships index, property, value, _

            relationship = null
            if relationships and relationships.length > 0
                relationship = relationships[0]
            return relationship

        catch error
            throw adjustError error

    #
    # Fetch and "return" (via callback) the relationships indexed under the
    # given property and value in the given index. If no such relationships
    # exist, an empty array is returned.
    #
    # @note This method will return multiple relationships if there are
    #   multiple hits. See {#getIndexedRelationship} for returning at most one
    #   relationship.
    #
    # @param index {String} The name of the index, e.g. `relationship_auto_index`.
    # @param property {String} The name of the property, e.g. `favorite`.
    # @param value {Object} The value of the property, e.g. `true`.
    # @param callback {Function}
    # @return {Array<Relationship>}
    #
    getIndexedRelationships: (index, property, value, _) ->
        try
            services = @getServices _

            key = encodeURIComponent property
            val = encodeURIComponent value
            url = "#{services.relationship_index}/#{index}/#{key}/#{val}"

            response = @_request.get url, _

            if response.statusCode isnt status.OK
                # Database error
                throw response

            # Success
            relationshipArray = JSON.parse response.body
            relationships = relationshipArray.map (relationship) =>
                new Relationship this, relationship
            return relationships

        catch error
            throw adjustError error

    #
    # Fetch and "return" (via callback) the relationship with the given Neo4j
    # ID. Throws an error if no relationship exists with this ID.
    #
    # @todo Should this indeed throw an error if no relationship exists with
    #   this ID? Or should we be returning undefined?
    #
    # @param id {Number} The integer ID of the relationship, e.g. `1234`.
    # @param callback {Function}
    # @return {Relationship}
    #
    getRelationshipById: (id, _) ->
        services = @getServices _
        # FIXME: Neo4j doesn't expose the path to relationships
        relationshipURL = services.node.replace('node', 'relationship')
        url = "#{relationshipURL}/#{id}"
        @getRelationship url, _

    ### Misc/Other: ###

    #
    # Fetch and "return" (via callback) the results of the given
    # {http://docs.neo4j.org/chunked/stable/cypher-query-lang.html Cypher}
    # query, optionally passing the given query parameters (recommended to
    # avoid Cypher injection security vulnerabilities). The returned results
    # are an array of "rows" (matches), where each row is a map from key name
    # (as given in the query) to value. Any values that represent nodes or
    # relationships are returned as {Node} and {Relationship} instances.
    #
    # @overload query(query, callback)
    #   @param query {String} The Cypher query. Can be multi-line.
    #   @param callback {Function}
    #   @return {Array<Object>}
    #
    # @overload query(query, params, callback)
    #   @param query {String} The Cypher query. Can be multi-line.
    #   @param params {Object} A map of parameters for the Cypher query.
    #   @param callback {Function}
    #   @return {Array<Object>}
    #   @example Fetch a user's likes.
    #     var query = [
    #       'START user=node({userId})',
    #       'MATCH (user) -[:likes]-> (other)',
    #       'RETURN other'
    #     ].join('\n');
    #     var params = {
    #       userId: currentUser.id
    #     };
    #     db.query(query, params, function (err, results) {
    #       if (err) throw err;
    #       var likes = results.map(function (result) {
    #         return result['other'];
    #       });
    #       // ...
    #     });
    #
    query: (query, params, _) ->
        try
            services = @getServices _
            endpoint = services.cypher or
                services.extensions?.CypherPlugin?['execute_query']

            if not endpoint
                throw new Error 'Cypher plugin not installed'

            response = @_request.post
                uri: endpoint
                json: if params then {query, params} else {query}
            , _

            # XXX workaround for neo4j silent failures for invalid queries:
            if response.statusCode is status.NO_CONTENT
                throw new Error """
                    Unknown Neo4j error for query:

                    #{query}

                """

            if response.statusCode isnt status.OK
                # Database error
                throw response

            # Success: build result maps, and transform nodes/relationships
            body = response.body    # JSON already parsed by request
            columns = body.columns
            results = for row in body.data
                map = {}
                for value, i in row
                    map[columns[i]] =
                        if value and typeof value is 'object' and value.self
                            if value.type then new Relationship this, value
                            else new Node this, value
                        else if value and typeof value is 'object' and value instanceof Array
                            for val in value
                                if val and typeof val is 'object' and val.self
                                    if val.type then new Relationship this, val
                                    else new Node this, val
                                else
                                    val
                        else
                            value
                map
            return results

        catch error
            throw adjustError error

    # XXX temporary backwards compatibility shim for query() argument order:
    do (actual = @::query) =>
        @::query = (query, params, callback) ->
            if typeof query is 'function' and typeof params is 'string'
                # instantiate a new error to derive the current stack, and
                # show the relevant source line in a warning:
                console.warn 'neo4j.GraphDatabase::query()’s signature is ' +
                    'now (query, params, callback). Please update your code!\n' +
                    new Error().stack.split('\n')[2]    # includes indentation
                callback = query
                query = params
                params = null
            else if typeof params is 'function'
                callback = params
                params = null

            actual.call @, query, params, callback

    #
    # Fetch and "return" (via callback) the nodes matching the given query (in
    # {http://lucene.apache.org/java/3_1_0/queryparsersyntax.html Lucene
    # syntax}) from the given index. If no such nodes exist, an empty array is
    # returned.
    #
    # @todo Implement a similar method for relationships?
    #
    # @param index {String} The name of the index, e.g. `node_auto_index`.
    # @param query {String} The Lucene query, e.g. `foo:bar AND hello:world`.
    # @param callback {Function}
    # @return {Array<Node>}
    #
    queryNodeIndex: (index, query, _) ->
        try
            services = @getServices _
            url = "#{services.node_index}/#{index}?query=#{encodeURIComponent query}"

            response = @_request.get url, _

            if response.statusCode isnt status.OK
                # Database error
                throw response

            # Success
            nodeArray = JSON.parse response.body
            nodes = nodeArray.map (node) =>
                new Node this, node
            return nodes

        catch error
            throw adjustError error
