# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for the RDF functionality in the Neptune database."""

import pytest
from awslabs.amazon_neptune_mcp_server.graph_store.database import NeptuneDatabase
from awslabs.amazon_neptune_mcp_server.models import (
    RDFGraphSchema,
)
from unittest.mock import MagicMock, patch


class TestRDFFunctionality:
    """Test class for the RDF functionality in the NeptuneDatabase class."""

    @patch('awslabs.amazon_neptune_mcp_server.graph_store.database.NeptuneDatabase._query_sparql')
    @patch('awslabs.amazon_neptune_mcp_server.graph_store.database.boto3.Session')
    @patch('awslabs.amazon_neptune_mcp_server.graph_store.database.requests.request')
    def test_get_local_name(self, mock_request, mock_session, mock_query_sparql):
        """Test the _get_local_name method for extracting local names from IRIs.

        This test verifies that:
        1. IRIs with '#' are correctly split into prefix and local name
        2. IRIs with '/' are correctly split into prefix and local name
        3. Invalid IRIs raise a ValueError
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'
        mock_session.return_value = mock_session_instance
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client

        # Mock the get_lpg_schema and get_rdf_schema methods to avoid actual API calls
        mock_client.get_propertygraph_summary.return_value = {
            'payload': {'graphSummary': {'nodeLabels': [], 'edgeLabels': []}}
        }
        mock_client.get_rdf_graph_summary.return_value = {
            'payload': {'graphSummary': {'classes': [], 'predicates': []}}
        }

        # Mock the _query_sparql method to avoid SigV4Auth issues
        mock_query_sparql.return_value = {'results': {'bindings': []}}

        # Create a NeptuneDatabase instance
        db = NeptuneDatabase('test-host')

        # Act & Assert - Test with '#'
        prefix, local = db._get_local_name('http://example.org/ontology#Person')
        assert prefix == 'http://example.org/ontology#'
        assert local == 'Person'

        # Act & Assert - Test with '/'
        prefix, local = db._get_local_name('http://example.org/ontology/Person')
        assert prefix == 'http://example.org/ontology/'
        assert local == 'Person'

        # Act & Assert - Test with invalid IRI
        with pytest.raises(
            ValueError, match="Unexpected IRI 'invalid-iri', contains neither '#' nor '/'"
        ):
            db._get_local_name('invalid-iri')

    @patch('awslabs.amazon_neptune_mcp_server.graph_store.database.NeptuneDatabase._query_sparql')
    @patch('awslabs.amazon_neptune_mcp_server.graph_store.database.boto3.Session')
    @patch('awslabs.amazon_neptune_mcp_server.graph_store.database.requests.request')
    def test_query_sparql(self, mock_request, mock_session, mock_query_sparql):
        """Test the query_sparql method for executing SPARQL queries.

        This test verifies that:
        1. The _query_sparql method is called with the correct query
        2. The result from the _query_sparql method is returned unchanged
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'
        mock_session.return_value = mock_session_instance
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client

        # Mock the get_lpg_schema and get_rdf_schema methods to avoid actual API calls
        mock_client.get_propertygraph_summary.return_value = {
            'payload': {'graphSummary': {'nodeLabels': [], 'edgeLabels': []}}
        }
        mock_client.get_rdf_graph_summary.return_value = {
            'payload': {'graphSummary': {'classes': [], 'predicates': []}}
        }

        # Mock the _query_sparql method to avoid SigV4Auth issues
        mock_query_sparql.return_value = {'results': {'bindings': []}}

        # Create a NeptuneDatabase instance
        db = NeptuneDatabase('test-host')

        # Reset the mock to track new calls
        mock_query_sparql.reset_mock()

        # Act
        query = 'SELECT * WHERE { ?s ?p ?o } LIMIT 10'
        result = db.query_sparql(query)

        # Assert
        mock_query_sparql.assert_called_once_with(query)
        assert result == {'results': {'bindings': []}}

    @patch('awslabs.amazon_neptune_mcp_server.graph_store.database.NeptuneDatabase._query_sparql')
    @patch('awslabs.amazon_neptune_mcp_server.graph_store.database.boto3.Session')
    @patch('awslabs.amazon_neptune_mcp_server.graph_store.database.requests.request')
    def test_get_rdf_schema(self, mock_request, mock_session, mock_query_sparql):
        """Test the get_rdf_schema method for retrieving the RDF schema.

        This test verifies that:
        1. The get_rdf_graph_summary API is called to get the RDF schema
        2. The _query_sparql method is called with the correct SPARQL query
        3. The RDF schema is correctly constructed from the API and query results
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'
        mock_session.return_value = mock_session_instance
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client

        # Mock the get_lpg_schema method to avoid actual API calls
        mock_client.get_propertygraph_summary.return_value = {
            'payload': {'graphSummary': {'nodeLabels': [], 'edgeLabels': []}}
        }

        # Mock the get_rdf_graph_summary API response
        mock_client.get_rdf_graph_summary.return_value = {
            'payload': {
                'graphSummary': {
                    'classes': ['http://example.org/Person', 'http://example.org/Organization'],
                    'predicates': [
                        {'http://example.org/name': {}},
                        {'http://example.org/worksFor': {}},
                    ],
                }
            }
        }

        # Mock the SPARQL query response
        mock_query_sparql.return_value = {
            'results': {
                'bindings': [
                    {
                        's': {'value': 'http://example.org/ontology'},
                        'p': {'value': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'},
                        'o': {'value': 'http://www.w3.org/2002/07/owl#Ontology'},
                    },
                    {
                        's': {'value': 'http://example.org/ontology'},
                        'p': {'value': 'http://www.w3.org/2000/01/rdf-schema#label'},
                        'o': {'value': 'Example Ontology'},
                    },
                    {
                        's': {'value': 'http://example.org/Person'},
                        'p': {'value': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'},
                        'o': {'value': 'http://www.w3.org/2002/07/owl#Class'},
                    },
                    {
                        's': {'value': 'http://example.org/Person'},
                        'p': {'value': 'http://www.w3.org/2000/01/rdf-schema#label'},
                        'o': {'value': 'Person'},
                    },
                    {
                        's': {'value': 'http://example.org/name'},
                        'p': {'value': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'},
                        'o': {'value': 'http://www.w3.org/2002/07/owl#DatatypeProperty'},
                    },
                    {
                        's': {'value': 'http://example.org/name'},
                        'p': {'value': 'http://www.w3.org/2000/01/rdf-schema#domain'},
                        'o': {'value': 'http://example.org/Person'},
                    },
                    {
                        's': {'value': 'http://example.org/worksFor'},
                        'p': {'value': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'},
                        'o': {'value': 'http://www.w3.org/2002/07/owl#ObjectProperty'},
                    },
                    {
                        's': {'value': 'http://example.org/worksFor'},
                        'p': {'value': 'http://www.w3.org/2000/01/rdf-schema#domain'},
                        'o': {'value': 'http://example.org/Person'},
                    },
                    {
                        's': {'value': 'http://example.org/worksFor'},
                        'p': {'value': 'http://www.w3.org/2000/01/rdf-schema#range'},
                        'o': {'value': 'http://example.org/Organization'},
                    },
                ]
            }
        }

        # Create a NeptuneDatabase instance
        db = NeptuneDatabase('test-host')

        # Reset the rdf_schema to None to force a refresh
        db.rdf_schema = None

        # Reset the mock to track new calls
        mock_query_sparql.reset_mock()

        # Act
        schema = db.get_rdf_schema()

        # Assert
        assert isinstance(schema, RDFGraphSchema)
        assert len(schema.rdfclasses) == 2
        assert 'http://example.org/Person' in schema.rdfclasses
        assert 'http://example.org/Organization' in schema.rdfclasses

        assert len(schema.predicates) == 2
        assert 'http://example.org/name' in schema.predicates
        assert 'http://example.org/worksFor' in schema.predicates

        assert len(schema.ontologies) == 1
        assert schema.ontologies[0].uri == 'http://example.org/ontology'
        assert schema.ontologies[0].label == 'Example Ontology'

        assert len(schema.classes) == 1
        assert schema.classes[0].uri == 'http://example.org/Person'
        assert schema.classes[0].local == 'Person'

        assert len(schema.dtprops) == 1
        assert schema.dtprops[0].uri == 'http://example.org/name'
        assert schema.dtprops[0].domain_uri == 'http://example.org/Person'

        assert len(schema.oprops) == 1
        assert schema.oprops[0].uri == 'http://example.org/worksFor'
        assert schema.oprops[0].domain_uri == 'http://example.org/Person'
        assert schema.oprops[0].range_uri == 'http://example.org/Organization'

        assert len(schema.rels) == 1
        assert schema.rels[0].uri == 'http://example.org/worksFor'
        assert schema.rels[0].local == 'worksFor'

        # Verify that _query_sparql was called
        mock_query_sparql.assert_called_once()

    @patch('awslabs.amazon_neptune_mcp_server.graph_store.database.NeptuneDatabase._query_sparql')
    @patch('awslabs.amazon_neptune_mcp_server.graph_store.database.boto3.Session')
    @patch('awslabs.amazon_neptune_mcp_server.graph_store.database.requests.request')
    def test_get_rdf_schema_cached(self, mock_request, mock_session, mock_query_sparql):
        """Test that get_rdf_schema returns the cached schema if available.

        This test verifies that:
        1. When rdf_schema is already set, it is returned without making API calls
        2. The cached schema is returned unchanged
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'
        mock_session.return_value = mock_session_instance
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client

        # Mock the get_lpg_schema and get_rdf_schema methods to avoid actual API calls
        mock_client.get_propertygraph_summary.return_value = {
            'payload': {'graphSummary': {'nodeLabels': [], 'edgeLabels': []}}
        }
        mock_client.get_rdf_graph_summary.return_value = {
            'payload': {'graphSummary': {'classes': [], 'predicates': []}}
        }

        # Mock the _query_sparql method to avoid SigV4Auth issues
        mock_query_sparql.return_value = {'results': {'bindings': []}}

        # Create a NeptuneDatabase instance
        db = NeptuneDatabase('test-host')

        # Create a mock schema
        mock_schema = RDFGraphSchema(
            distinct_prefixes={'http://example.org/': 'ex'},
            rdfclasses=['http://example.org/Person'],
            predicates=['http://example.org/name'],
        )

        # Set the cached schema
        db.rdf_schema = mock_schema

        # Reset the mocks to track new calls
        mock_client.get_rdf_graph_summary.reset_mock()
        mock_query_sparql.reset_mock()

        # Act
        schema = db.get_rdf_schema()

        # Assert
        assert schema == mock_schema
        mock_client.get_rdf_graph_summary.assert_not_called()  # Should not be called again
        mock_query_sparql.assert_not_called()  # Should not be called again
