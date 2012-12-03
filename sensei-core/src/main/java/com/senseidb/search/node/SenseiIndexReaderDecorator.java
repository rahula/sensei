/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
package com.senseidb.search.node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.senseidb.conf.SenseiFacetHandlerBuilder;
import com.senseidb.conf.SenseiSchema;
import com.senseidb.conf.SenseiSchema.FacetDefinition;
import com.senseidb.conf.SenseiSchema.FieldDefinition;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.plugin.PluggableSearchEngineManager;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;

import org.apache.lucene.index.IndexReader;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.impl.indexing.AbstractIndexReaderDecorator;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.RuntimeFacetHandlerFactory;

public class SenseiIndexReaderDecorator extends AbstractIndexReaderDecorator<BoboIndexReader> {
	private final List<FacetHandler<?>> _facetHandlers;
	private static final Logger logger = Logger.getLogger(SenseiIndexReaderDecorator.class);
    private final List<RuntimeFacetHandlerFactory<?,?>> _facetHandlerFactories;
    private final SenseiPluginRegistry _pluginRegistry;
    private  final PluggableSearchEngineManager _pluggableSearchEngineManager;
    Map<com.senseidb.conf.SenseiSchema.FacetDefinition, SenseiSchema.FieldDefinition> _regexFacets;

	public SenseiIndexReaderDecorator(List<FacetHandler<?>> facetHandlers,
                                      List<RuntimeFacetHandlerFactory<?,?>> facetHandlerFactories,
                                      SenseiSchema schema,
                                      SenseiPluginRegistry pluginRegistry,
                                      PluggableSearchEngineManager pluggableSearchEngineManager)
	{
        _facetHandlers = facetHandlers;
        _facetHandlerFactories = facetHandlerFactories;
        _pluginRegistry = pluginRegistry;
        _pluggableSearchEngineManager = pluggableSearchEngineManager;
        _regexFacets = new HashMap<SenseiSchema.FacetDefinition, SenseiSchema.FieldDefinition>();

        Map<String, SenseiSchema.FieldDefinition> fields = schema.getFieldDefMap();

        for (SenseiSchema.FacetDefinition facetDefinition : schema.getFacets()) {
            SenseiSchema.FieldDefinition field = fields.get(facetDefinition.column);
            if (field != null && field.hasRegex) {
                _regexFacets.put(facetDefinition, field);
            }
        }
	}
	
	public List<FacetHandler<?>> getFacetHandlerList(){
		return _facetHandlers;
	}
	
	public List<RuntimeFacetHandlerFactory<?,?>> getFacetHandlerFactories(){
		return _facetHandlerFactories;
	}


	public BoboIndexReader decorate(ZoieIndexReader<BoboIndexReader> zoieReader) throws IOException {
		BoboIndexReader boboReader = null;

        if (zoieReader != null) {
            List<FacetHandler<?>> facetHandlers = _facetHandlers;
            List<RuntimeFacetHandlerFactory<?,?>> facetHandlerFactories = _facetHandlerFactories;

            if (!_regexFacets.isEmpty()) {
                facetHandlers = new ArrayList<FacetHandler<?>>(_facetHandlers);
                facetHandlerFactories = new ArrayList<RuntimeFacetHandlerFactory<?, ?>>(_facetHandlerFactories);
                Collection<String> indexFields = zoieReader.getFieldNames(IndexReader.FieldOption.ALL);
                addRegexFacets(facetHandlers, facetHandlerFactories, indexFields);
            }

            boboReader = BoboIndexReader.getInstanceAsSubReader(zoieReader,facetHandlers, facetHandlerFactories);
        }
        return boboReader;
	}

    /**
     * Add facethandlers by matching the regex in a facet name with all the field names in the index.
     * @param facetHandlers store for facet handlers
     * @param facetHandlerFactories store for runtime facet handlers.
     * @param indexFields   index fields to match
     */
    private void addRegexFacets(List<FacetHandler<?>> facetHandlers,
                                List<RuntimeFacetHandlerFactory<?,?>>  facetHandlerFactories,
                                Collection<String> indexFields )  {

        for (FacetDefinition regexFacet: _regexFacets.keySet()) {
            FieldDefinition field = _regexFacets.get(regexFacet);

            for (String indexField : indexFields) {
                if (field.regexPattern.matcher(indexField).matches()) {
                    try {
                        logger.debug("field: " + field.name + " matches facet: " + regexFacet.name );
                        FacetDefinition facet = createRegularFacet(indexField, regexFacet);
                        SenseiFacetHandlerBuilder.getFacetHandler(
                                facet, field,
                                facetHandlers,
                                facetHandlerFactories,
                                _pluginRegistry);

                    } catch (ConfigurationException e) {
                        logger.error(e, e);
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        if (logger.isDebugEnabled()) {
            Set<FacetHandler<?>> newFacets = new HashSet<FacetHandler<?>>(facetHandlers);
            newFacets.removeAll(_facetHandlers);
            for (FacetHandler<?> facet : newFacets) {
                logger.debug("Added Dynamic facets: " + facet.getName());
            }
        }
    }

    /**
     * Create a facet definition by replacing the regex in the name with the actual index field name.
     * @param indexFieldName Actual field name that satisfies the facet's regex.
     * @param regexFacet   a facet with a regex in its name.
     * @return
     */
    FacetDefinition createRegularFacet(String indexFieldName, FacetDefinition regexFacet) {
        FacetDefinition facet = new FacetDefinition();
        // set the name and column values to the index field name
        facet.name = indexFieldName;
        facet.column = indexFieldName;
        // copy the values of existing fields
        facet.type = regexFacet.type;
        facet.dynamic = regexFacet.dynamic;
        facet.params = regexFacet.params;
        facet.dependSet = regexFacet.dependSet;
        return facet;
    }

	@Override
    public BoboIndexReader redecorate(BoboIndexReader reader, ZoieIndexReader<BoboIndexReader> newReader,boolean withDeletes)
                          throws IOException {
          return reader.copy(newReader);
    }
}

