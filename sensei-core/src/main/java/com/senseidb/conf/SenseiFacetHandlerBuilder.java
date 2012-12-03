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
package com.senseidb.conf;

import java.text.ParseException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.util.Assert;

import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.FacetHandler.FacetDataNone;
import com.browseengine.bobo.facets.FacetHandlerInitializerParam;
import com.browseengine.bobo.facets.RuntimeFacetHandler;
import com.browseengine.bobo.facets.RuntimeFacetHandlerFactory;
import com.browseengine.bobo.facets.AbstractRuntimeFacetHandlerFactory;
import com.browseengine.bobo.facets.attribute.AttributesFacetHandler;
import com.browseengine.bobo.facets.data.PredefinedTermListFactory;
import com.browseengine.bobo.facets.data.TermListFactory;
import com.browseengine.bobo.facets.impl.CompactMultiValueFacetHandler;
import com.browseengine.bobo.facets.impl.DynamicTimeRangeFacetHandler;
import com.browseengine.bobo.facets.impl.HistogramFacetHandler;
import com.browseengine.bobo.facets.impl.MultiValueFacetHandler;
import com.browseengine.bobo.facets.impl.MultiValueWithWeightFacetHandler;
import com.browseengine.bobo.facets.impl.PathFacetHandler;
import com.browseengine.bobo.facets.impl.RangeFacetHandler;
import com.browseengine.bobo.facets.impl.SimpleFacetHandler;
import com.browseengine.bobo.facets.range.MultiRangeFacetHandler;
import com.senseidb.conf.SenseiSchema.FacetDefinition;
import com.senseidb.conf.SenseiSchema.FieldDefinition;
import com.senseidb.indexing.DefaultSenseiInterpreter;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.facet.UIDFacetHandler;
import com.senseidb.search.plugin.PluggableSearchEngineManager;
import com.senseidb.search.req.SenseiSystemInfo;
import com.senseidb.search.req.SenseiSystemInfo.SenseiFacetInfo;

public class SenseiFacetHandlerBuilder {

    private static Logger logger = Logger
            .getLogger(SenseiFacetHandlerBuilder.class);

    public static String UID_FACET_NAME = "_uid";

    /**
     * Create a new {@link TermListFactory} for a given facet
     * @param facetDef facet definition for which to create a term list factory.
     * @param fieldDef corresponding field definition for the facet.
     */
    private static  TermListFactory<?> getTermListFactory(FacetDefinition facetDef, FieldDefinition fieldDef) {
        String type = facetDef.type;

        if (type.equals("int")) {
            return  DefaultSenseiInterpreter.getTermListFactory(int.class);
        } else if (type.equals("short")) {
            return DefaultSenseiInterpreter.getTermListFactory(short.class);
        } else if (type.equals("long")) {
            return DefaultSenseiInterpreter.getTermListFactory(long.class);
        } else if (type.equals("float")) {
            return DefaultSenseiInterpreter.getTermListFactory(float.class);
        } else if (type.equals("double")) {
            return DefaultSenseiInterpreter.getTermListFactory(double.class);
        } else if (type.equals("char")) {
            return DefaultSenseiInterpreter.getTermListFactory(char.class);
        } else if (type.equals("string")) {
            return TermListFactory.StringListFactory;
        } else if (type.equals("boolean")) {
            return DefaultSenseiInterpreter.getTermListFactory(boolean.class);
        } else if (type.equals("date")) {
            String f = fieldDef.formatString;
            if (f.isEmpty())
                throw new RuntimeException("Date format cannot be empty.");
            else
                return new PredefinedTermListFactory<Date>(Date.class, f);
        }
        return null;
    }

    static SimpleFacetHandler buildSimpleFacetHandler(String name, String fieldName, Set<String> depends, TermListFactory<?> termListFactory) {
        return new SimpleFacetHandler(name, fieldName, termListFactory, depends);
    }

    static CompactMultiValueFacetHandler buildCompactMultiHandler(String name, String fieldName, Set<String> depends, TermListFactory<?> termListFactory) {
        // compact multi should honor depends
        return new CompactMultiValueFacetHandler(name, fieldName, termListFactory);
    }

    static MultiValueFacetHandler buildMultiHandler(String name, String fieldName, TermListFactory<?> termListFactory, Set<String> depends) {
        return new MultiValueFacetHandler(name, fieldName, termListFactory, null, depends);
    }

    static MultiValueFacetHandler buildWeightedMultiHandler(String name, String fieldName, TermListFactory<?> termListFactory, Set<String> depends) {
        return new MultiValueWithWeightFacetHandler(name, fieldName, termListFactory);
    }

    static PathFacetHandler buildPathHandler(String name, String fieldName, Map<String, List<String>> paramMap) {
        PathFacetHandler handler = new PathFacetHandler(name, fieldName, false);    // path does not support multi value yet
        String sep = null;
        if (paramMap != null) {
            List<String> sepVals = paramMap.get("separator");
            if (sepVals != null && sepVals.size() > 0) {
                sep = sepVals.get(0);
            }
        }
        if (sep != null) {
            handler.setSeparator(sep);
        }
        return handler;
    }

    static RangeFacetHandler buildRangeHandler(String name, String fieldName, TermListFactory<?> termListFactory, Map<String, List<String>> paramMap) {
        LinkedList<String> predefinedRanges = buildPredefinedRanges(paramMap);
        return new RangeFacetHandler(name, fieldName, termListFactory, predefinedRanges);
    }

    private static LinkedList<String> buildPredefinedRanges(Map<String, List<String>> paramMap) {
        LinkedList<String> predefinedRanges = new LinkedList<String>();
        if (paramMap != null) {
            List<String> rangeList = paramMap.get("range");
            if (rangeList != null) {
                for (String range : rangeList) {
                    if (!range.matches("\\[.* TO .*\\]")) {
                        if (!range.contains("-") || !range.contains(",")) {
                            range = "[" + range.replaceFirst("[-,]", " TO ") + "]";
                        } else {
                            range = "[" + range.replaceFirst(",", " TO ") + "]";
                        }
                    }
                    predefinedRanges.add(range);
                }
            }
        }
        return predefinedRanges;
    }

    public static Map<String, List<String>> parseParams(JSONArray paramList) throws JSONException {
        HashMap<String, List<String>> retmap = new HashMap<String, List<String>>();
        if (paramList != null) {
            int count = paramList.length();
            for (int j = 0; j < count; ++j) {
                JSONObject param = paramList.getJSONObject(j);
                String paramName = param.getString("name");
                String paramValue = param.getString("value");

                List<String> list = retmap.get(paramName);
                if (list == null) {
                    list = new LinkedList<String>();
                    retmap.put(paramName, list);
                }

                list.add(paramValue);
            }
        }

        return retmap;
    }

    private static String getRequiredSingleParam(Map<String, List<String>> paramMap,
                                                 String name)
            throws ConfigurationException {
        if (paramMap != null) {
            List<String> vals = paramMap.get(name);
            if (vals != null && vals.size() > 0) {
                return vals.get(0);
            } else {
                throw new ConfigurationException("Parameter " + name + " is missing.");
            }
        } else {
            throw new ConfigurationException("Parameter map is null.");
        }
    }

    private static RuntimeFacetHandlerFactory<?, ?> getHistogramFacetHandlerFactory(String name,
                                                                                    Map<String, List<String>> paramMap)
            throws ConfigurationException {
        String dataType = getRequiredSingleParam(paramMap, "datatype");
        String dataHandler = getRequiredSingleParam(paramMap, "datahandler");
        String startParam = getRequiredSingleParam(paramMap, "start");
        String endParam = getRequiredSingleParam(paramMap, "end");
        String unitParam = getRequiredSingleParam(paramMap, "unit");

        if ("int".equals(dataType)) {
            int start = Integer.parseInt(startParam);
            int end = Integer.parseInt(endParam);
            int unit = Integer.parseInt(unitParam);
            return buildHistogramFacetHandlerFactory(name, dataHandler, start, end, unit);
        } else if ("short".equals(dataType)) {
            short start = (short) Integer.parseInt(startParam);
            short end = (short) Integer.parseInt(endParam);
            short unit = (short) Integer.parseInt(unitParam);
            return buildHistogramFacetHandlerFactory(name, dataHandler, start, end, unit);
        } else if ("long".equals(dataType)) {
            long start = Long.parseLong(startParam);
            long end = Long.parseLong(endParam);
            long unit = Long.parseLong(unitParam);
            return buildHistogramFacetHandlerFactory(name, dataHandler, start, end, unit);
        } else if ("float".equals(dataType)) {
            float start = Float.parseFloat(startParam);
            float end = Float.parseFloat(endParam);
            float unit = Float.parseFloat(unitParam);
            return buildHistogramFacetHandlerFactory(name, dataHandler, start, end, unit);
        } else if ("double".equals(dataType)) {
            double start = Double.parseDouble(startParam);
            double end = Double.parseDouble(endParam);
            double unit = Double.parseDouble(unitParam);
            return buildHistogramFacetHandlerFactory(name, dataHandler, start, end, unit);
        }
        return null;
    }

    private static <T extends Number> RuntimeFacetHandlerFactory<?, ?> buildHistogramFacetHandlerFactory(final String name,
                                                                                                         final String dataHandler,
                                                                                                         final T start,
                                                                                                         final T end,
                                                                                                         final T unit) {
        return new AbstractRuntimeFacetHandlerFactory<FacetHandlerInitializerParam, RuntimeFacetHandler<FacetDataNone>>() {
            @Override
            public RuntimeFacetHandler<FacetDataNone> get(FacetHandlerInitializerParam params) {
                return new HistogramFacetHandler<T>(name, dataHandler, start, end, unit);
            }

            ;

            @Override
            public boolean isLoadLazily() {
                return true;
            }

            @Override
            public String getName() {
                return name;
            }
        };
    }

    /**
     *
     * Creates {@link FacetHandler} and or {@link RuntimeFacetHandlerFactory} for a
     * give facet.
     * @param facet facet definition for which to create facet handlers
     * @param field field definition corresponding to the facet.
     * @param facets Any new facet handlers created will be added to this list.
     * @param runtimeFacets Any facet handler factories will be added to this list.
     * @return   {@link SenseiFacetInfo} containing information about the facet handler.
     */
    public static SenseiFacetInfo getFacetHandler(FacetDefinition facet,
                                                  FieldDefinition field,
                                                  List<FacetHandler<?>> facets,
                                                  List<RuntimeFacetHandlerFactory<?, ?>> runtimeFacets,
                                                  SenseiPluginRegistry pluginRegistry) throws  ConfigurationException {
        String type = facet.type;
        String fieldName = facet.column;
        Set<String> dependSet = facet.dependSet;

        SenseiFacetInfo facetInfo = new SenseiFacetInfo(facet.name);
        Map<String, String> facetProps = new HashMap<String, String>();
        facetProps.put("type", type);
        facetProps.put("column", fieldName);
        facetProps.put("column_type", field.columnType);
        facetProps.put("depends", dependSet.toString());
        Map<String, List<String>> paramMap = facet.params;
        facetInfo.setProps(facetProps);

        FacetHandler<?> facetHandler = null;
        if (type.equals("simple")) {
            facetHandler = buildSimpleFacetHandler(facet.name, fieldName, dependSet, getTermListFactory(facet, field));
        } else if (type.equals("path")) {
            facetHandler = buildPathHandler(facet.name, fieldName, paramMap);
        } else if (type.equals("range")) {
            if (field.isMulti) {
                facetHandler = new MultiRangeFacetHandler(facet.name, fieldName, null,
                        getTermListFactory(facet, field), buildPredefinedRanges(paramMap));
            } else {
            facetHandler = buildRangeHandler(facet.name, fieldName,
                    getTermListFactory(facet, field), paramMap);
            }
        } else if (type.equals("multi")) {
            facetHandler = buildMultiHandler(facet.name, fieldName, getTermListFactory(facet, field),
                    dependSet);
        } else if (type.equals("compact-multi")) {
            facetHandler = buildCompactMultiHandler(facet.name, fieldName, dependSet,
                    getTermListFactory(facet, field));
        } else if (type.equals("weighted-multi")) {
            facetHandler = buildWeightedMultiHandler(facet.name, fieldName, getTermListFactory(facet, field)
                    , dependSet);
        } else if (type.equals("attribute")) {
            facetHandler = new AttributesFacetHandler(facet.name, fieldName,
                    getTermListFactory(facet, field),
                    null, facetProps);
        } else if (type.equals("histogram")) {
            // A histogram facet handler is always dynamic
            RuntimeFacetHandlerFactory<?, ?> runtimeFacetFactory = getHistogramFacetHandlerFactory(facet.name, paramMap);
            runtimeFacets.add(runtimeFacetFactory);
            facetInfo.setRunTime(true);
        } else if (type.equals("dynamicTimeRange")) {
            if (dependSet.isEmpty()) {
                Assert.isTrue(fieldName != null && fieldName.length() > 0, "Facet handler " + facet.name
                        + " requires either depends or column attributes");
                RangeFacetHandler internalFacet = new RangeFacetHandler(facet.name + "_internal",
                        fieldName, new PredefinedTermListFactory(Long.class, DynamicTimeRangeFacetHandler.NUMBER_FORMAT), null);
                facets.add(internalFacet);
                dependSet.add(internalFacet.getName());
            }
            RuntimeFacetHandlerFactory<?, ?> runtimeFacetFactory = getDynamicTimeFacetHandlerFactory(facet.name, fieldName, dependSet, paramMap);
            runtimeFacets.add(runtimeFacetFactory);
            facetInfo.setRunTime(true);

        } else if (type.equals("custom")) {
            // Load from custom-facets spring configuration.
            if (facet.dynamic) {
                RuntimeFacetHandlerFactory<?, ?> runtimeFacetFactory = pluginRegistry.getRuntimeFacet(facet.name);
                runtimeFacets.add(runtimeFacetFactory);
                facetInfo.setRunTime(true);
            } else {
                facetHandler = pluginRegistry.getFacet(facet.name);
            }
        } else {
            throw new IllegalArgumentException("type not supported: " + type);
        }

        if (facetHandler != null) {
            facets.add(facetHandler);
        }
        return facetInfo;
    }

    /**
     * Create all facet handlers based on the SenseiSchema and PluggableSearchEngineManager.
     * @param schema {@link SenseiSchema} which has the facet and field definitions.
     * @param pluginRegistry {@link SenseiPluginRegistry} used for instantiating any custom facet hanlders.
     * @param facets Any {@link FacetHandler} created are added to this list.
     * @param runtimeFacets Any {@link RuntimeFacetHandler} created are added to this list.
     * @param pluggableSearchEngineManager A {@link PluggableSearchEngineManager} to add any FacetHandlers for facets
     *                                     defined outside of SenseiSchema.
     */
    public static SenseiSystemInfo buildFacets(SenseiSchema schema,
                                               SenseiPluginRegistry pluginRegistry,
                                               List<FacetHandler<?>> facets,
                                               List<RuntimeFacetHandlerFactory<?, ?>> runtimeFacets,
                                               PluggableSearchEngineManager pluggableSearchEngineManager)
            throws JSONException, ConfigurationException {

        SenseiSystemInfo sysInfo = new SenseiSystemInfo();

        Set<String> pluggableSearchEngineFacetNames = pluggableSearchEngineManager.getFacetNames();

        Map<String, FieldDefinition> fields = schema.getFieldDefMap();
        Set<SenseiSystemInfo.SenseiFacetInfo> facetInfos = new HashSet<SenseiSystemInfo.SenseiFacetInfo>();

        for (FacetDefinition facetDef: schema.getFacets()) {
            String name = facetDef.name;
            FieldDefinition fieldDef = fields.get(facetDef.column);

            if (fieldDef != null && fieldDef.hasRegex) {
                continue;
            }
            if (UID_FACET_NAME.equals(name)) {
                logger.error("facet name: " + UID_FACET_NAME + " is reserved, skipping...");
                continue;
            }
            if (pluggableSearchEngineFacetNames.contains(name)) {
                continue;
            }
            SenseiSystemInfo.SenseiFacetInfo facetInfo = getFacetHandler(facetDef, fieldDef, facets, runtimeFacets, pluginRegistry);
            facetInfos.add(facetInfo);
        }

        facets.addAll((Collection<? extends FacetHandler<?>>) pluggableSearchEngineManager.createFacetHandlers());
        // uid facet handler to be added by default
        UIDFacetHandler uidHandler = new UIDFacetHandler(UID_FACET_NAME);
        facets.add(uidHandler);
        sysInfo.setFacetInfos(facetInfos);

        return sysInfo;
    }

    public static RuntimeFacetHandlerFactory<?, ?> getDynamicTimeFacetHandlerFactory(final String name, String fieldName, Set<String> dependSet,
                                                                                     final Map<String, List<String>> paramMap) {

        Assert.isTrue(dependSet.size() == 1, "Facet handler " + name + " should rely only on exactly one another facet handler, but accodring to config the depends set is " + dependSet);
        final String depends = dependSet.iterator().next();
        Assert.notEmpty(paramMap.get("range"), "Facet handler " + name + " should have at least one predefined range");

        return new AbstractRuntimeFacetHandlerFactory<FacetHandlerInitializerParam, RuntimeFacetHandler<?>>() {

            @Override
            public String getName() {
                return name;
            }

            @Override
            public RuntimeFacetHandler<?> get(FacetHandlerInitializerParam params) {
                long overrideNow = -1;
                try {
                    String overrideProp = System.getProperty("override.now");
                    if (overrideProp != null) {
                        overrideNow = Long.parseLong(overrideProp);
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }

                long now = System.currentTimeMillis();
                if (overrideNow > 0)
                    now = overrideNow;
                else {
                    if (params != null) {
                        long[] longParam = params.getLongParam("now");
                        if (longParam == null || longParam.length == 0)
                            longParam = params.getLongParam("time");  // time will also work

                        if (longParam != null && longParam.length > 0)
                            now = longParam[0];
                    }
                }

                List<String> ranges = paramMap.get("range");

                try {
                    return new DynamicTimeRangeFacetHandler(name, depends, now, ranges);
                } catch (ParseException ex) {
                    throw new RuntimeException(ex);
                }
            }
        };
    }
}
