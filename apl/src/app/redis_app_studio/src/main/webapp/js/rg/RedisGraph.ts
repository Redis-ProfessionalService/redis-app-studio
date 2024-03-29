/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/// <reference path="../smartclient.d.ts"/>

// noinspection HtmlDeprecatedAttribute,UnnecessaryLocalVariableJS
/**
 * The Redis App Studio Data Modeler Application is responsible for managing
 * an in-memory grid for a user who is interested in modeling their data
 * set prior to storing it in Redis. The Data Modeler class defines and
 * manages the complete SmartClient application. While this logic could
 * be broken out to other smaller files, the decision was made to keep it
 * self-contained to simplify application generation and minimize browser
 * load times.
 */
class RedisGraph
{
    protected appLayout: isc.VStack;
    protected appContext: AppContext;

    constructor(anAppContext: AppContext)
    {
        this.appContext = anAppContext;
    }

    private createHTMLHeader(): string
    {
        const windowAppContext = (window as any)._appContext_;

        let appName: string;
        if (windowAppContext.isModelerEnabled())
            appName = windowAppContext.getAppName();
        else
            appName = "Application Launcher";

        return "<table class=\"ahTable\"> <col width=\"1%\"> <col width=\"5%\"> <col width=\"15%\">" +
            " <col width=\"58%\"> <col width=\"20%\"> <col width=\"1%\"> <tr> <td>&nbsp;</td>" +
            "  <td><img alt=\"Redis App Studio\" class=\"ahImage\" src=\"images/redis-app-studio.svg\" height=\"99\" width=\"60\"></td>" +
            "  <td class=\"ahGroup\">" + windowAppContext.getGroupName() + "</td> <td>&nbsp;</td>" +
            "  <td class=\"ahName\">" + appName + "</td>\n" +
            "  <td>&nbsp;</td> </tr> </table>";
    }

    // Generates the application header section of the UI
    private createHeaderSection(): isc.HTMLFlow
    {
        let htmlString = this.createHTMLHeader();
        let headerHTMLFlow = isc.HTMLFlow.create({ID:"headerHTMLFlow", width:"100%", height: "5%", autoDraw: false, contents:htmlString});

        return headerHTMLFlow;
    }

    public defaultCriteria(aDSStructure: string, aDSTitle: string, aFetchLimit?: number): any
    {
        const windowAppContext = (window as any)._appContext_;

        let fetchLimit: number;
        if (aFetchLimit)
            fetchLimit = aFetchLimit;
        else
            fetchLimit = 100;
        let simpleCriteria = {
            _dsTitle: aDSTitle,
            _dsStructure: aDSStructure,
            _appPrefix: windowAppContext.getPrefix(),
            _fetchPolicy: windowAppContext.getFetchPolicy(),
            _offset: windowAppContext.getCriteriaOffset(),
            _limit: fetchLimit,
            _graphCriteriaCount: 0
        };

        return simpleCriteria;
    }

    // Handles main grid graph search operations
    private executeAppViewGraphSearch(): void
    {
        const minAdvancedCriteriaLength = 89;   // characters

        const windowAppContext = (window as any)._appContext_;
        let filterMap = new Map();
        filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
        filterMap.set("_dsStructure", windowAppContext.getDSStructure());
        filterMap.set("_appPrefix", windowAppContext.getPrefix());
        filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
        filterMap.set("_offset", windowAppContext.getCriteriaOffset());
        filterMap.set("_limit", windowAppContext.getCriteriaLimit());
        const windowSearchForm = (window as any).searchForm;
        let searchForm: isc.DynamicForm;
        searchForm = windowSearchForm;
        const windowGasGrid = (window as any).gasGrid;
        let gasGrid: isc.ListGrid;
        gasGrid = windowGasGrid;
        let listGridRecord : isc.ListGridRecord;
        let recordCount = gasGrid.data.getLength();
        filterMap.set("_dgCriterionCount", recordCount);
        for (let recordOffset = 0; recordOffset < recordCount; recordOffset++)
        {
            listGridRecord = gasGrid.data.get(recordOffset);
            // @ts-ignore
            filterMap.set("dgc_"+recordOffset+"_object_type", listGridRecord.object_type);
            // @ts-ignore
            filterMap.set("dgc_"+recordOffset+"_object_identifier", listGridRecord.object_identifier);
            // @ts-ignore
            filterMap.set("dgc_"+recordOffset+"_hop_count", listGridRecord.hop_count);
            // @ts-ignore
            filterMap.set("dgc_"+recordOffset+"_edge_direction", listGridRecord.edge_direction);
            // @ts-ignore
            if (listGridRecord.ds_criteria != null)
            {
                // @ts-ignore
                let acFlattened = isc.DataSource.flattenCriteria(listGridRecord.ds_criteria);
                let acJSON = isc.JSON.encode(acFlattened);
                if (acJSON.length > minAdvancedCriteriaLength)
                    filterMap.set("dgc_"+recordOffset+"_ds_criteria", acJSON);
            }
        }
        let searchTerm = searchForm.getValue("search_terms");
        filterMap.set("_search", searchTerm);
        let simpleCriteria = {};
        filterMap.forEach((value, key) => {
            // @ts-ignore
            simpleCriteria[key] = value;
        });
        const windowAppViewGrid = (window as any).appViewGrid;
        let appViewGrid: isc.ListGrid;
        appViewGrid = windowAppViewGrid;
        appViewGrid.invalidateCache();
        appViewGrid.filterData(simpleCriteria);
    }

    // Executes main grid export operations
    private executeAppViewGridExport(anAction: string, aFormat: string, aFetchLimit: number): void
    {
        const windowAppContext = (window as any)._appContext_;
        const windowAppViewGrid = (window as any).appViewGrid;
        let appViewGrid: isc.ListGrid;
        appViewGrid = windowAppViewGrid;
        const windowSearchForm = (window as any).searchForm;
        let searchForm: isc.DynamicForm;
        searchForm = windowSearchForm;
        const windowSearchFilter = (window as any).searchFilter;
        let searchFilter: isc.FilterBuilder;
        searchFilter = windowSearchFilter;
        let filterMap = new Map();
        filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
        filterMap.set("_dsStructure", windowAppContext.getDSStructure());
        filterMap.set("_appPrefix", windowAppContext.getPrefix());
        filterMap.set("_action", anAction);
        filterMap.set("_format", aFormat);
        filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
        filterMap.set("_offset", windowAppContext.getCriteriaOffset());
        filterMap.set("_limit", aFetchLimit);  // need to ensure page is filled with rows
        let searchTerm = searchForm.getValue("search_terms");
        filterMap.set("_search", searchTerm);
        let acFlattened = isc.DataSource.flattenCriteria(searchFilter.getCriteria());
        let acJSON = isc.JSON.encode(acFlattened);
        // isc.say("acJSON[" + acJSON.length + "] = " + acJSON);
        if (acJSON.length > 89)
            filterMap.set("_advancedCriteria", acJSON);
        let simpleCriteria = {};
        filterMap.forEach((value, key) => {
            // @ts-ignore
            simpleCriteria[key] = value;
        });
        let dsRequest = {
            ID: "dsRequest",
            operationId: "exportData",
            downloadResult: true,
            downloadToNewWindow: false
        };
        appViewGrid.fetchData(simpleCriteria, null, dsRequest);
        // Force a reload of the main grid after the download completes
        setTimeout(() => {  appViewGrid.filterData(simpleCriteria); }, 2000);
    }

    // Handles main grid search operations
    private executeAppViewGridSearch(): void
    {
        const minAdvancedCriteriaLength = 89;   // characters

        const windowAppContext = (window as any)._appContext_;
        let filterMap = new Map();
        filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
        filterMap.set("_dsStructure", windowAppContext.getDSStructure());
        filterMap.set("_appPrefix", windowAppContext.getPrefix());
        filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
        filterMap.set("_offset", windowAppContext.getCriteriaOffset());
        filterMap.set("_limit", windowAppContext.getCriteriaLimit());
        const windowSearchForm = (window as any).searchForm;
        let searchForm: isc.DynamicForm;
        searchForm = windowSearchForm;
        const windowSearchFilter = (window as any).searchFilter;
        let searchFilter: isc.FilterBuilder;
        searchFilter = windowSearchFilter;
        let searchTerm = searchForm.getValue("search_terms");
        filterMap.set("_search", searchTerm);
        // Use the following logic to bypass the SC Pro license restriction
        let acFlattened = isc.DataSource.flattenCriteria(searchFilter.getCriteria());
        let acJSON = isc.JSON.encode(acFlattened);
        // isc.say("acJSON[" + acJSON.length + "] = " + acJSON);
        if (acJSON.length > minAdvancedCriteriaLength)
            filterMap.set("_advancedCriteria", acJSON);
        let simpleCriteria = {};
        filterMap.forEach((value, key) => {
            // @ts-ignore
            simpleCriteria[key] = value;
        });
        const windowAppViewGrid = (window as any).appViewGrid;
        let appViewGrid: isc.ListGrid;
        appViewGrid = windowAppViewGrid;
        appViewGrid.invalidateCache();
        appViewGrid.filterData(simpleCriteria);
    }

    // Handles main grid fetch operations
    private executeAppViewGridFetch(): void
    {
        const minAdvancedCriteriaLength = 89;   // characters
        const windowAppContext = (window as any)._appContext_;

        let filterMap = new Map();
        filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
        filterMap.set("_dsStructure", windowAppContext.getDSStructure());
        filterMap.set("_appPrefix", windowAppContext.getPrefix());
        filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
        filterMap.set("_offset", windowAppContext.getCriteriaOffset());
        filterMap.set("_limit", windowAppContext.getCriteriaLimit());
        filterMap.set("_redisStorageType", windowAppContext.getRedisStorageType());  // row management
        let simpleCriteria = {};
        filterMap.forEach((value, key) => {
            // @ts-ignore
            simpleCriteria[key] = value;
        });
        const windowAppViewGrid = (window as any).appViewGrid;
        let appViewGrid: isc.ListGrid;
        appViewGrid = windowAppViewGrid;
        appViewGrid.invalidateCache();
        appViewGrid.filterData(simpleCriteria);
    }

    // Creates multiple methods for search the main grid graph data
    private createSearchSection(): isc.SectionStack
    {
        const windowAppContext = (window as any)._appContext_;

        // Criteria search section
        let searchForm = isc.DynamicForm.create({
                                        ID:"searchForm", autoDraw: false, iconWidth: 16, iconHeight: 16,
                                        items: [{
                                            type: "text", name: "search_terms", title: "Search Term(s)", wrapTitle: false, width: 300, suppressBrowserClearIcon:true,
                                            // @ts-ignore
                                            keyPress : function(item: isc.FormItem, form: isc.DynamicForm, keyName: isc.KeyName, characterValue: number) {
                                                if (keyName == "Enter")
                                                    RedisGraph.prototype.executeAppViewGraphSearch();
                                                return true;
                                            },
                                            icons: [{
                                                name: "clear", src: "[SKIN]actions/close.png", width: 10, height: 10, inline: true, inlineIconAlign: "right", prompt: "Clear Field Contents",
                                                // @ts-ignore
                                                click : function (form, item, icon) {
                                                    item.clearValue();
                                                    item.focusInItem();
                                                }
                                            }]
                                        }]
                                    });

        let gasNodeFilter = isc.FilterBuilder.create({
                                        ID: "gasNodeFilter", width: 500, height: 125, autoDraw: false,
                                        dataSource: windowAppContext.getAppPrefixDS("NodeFilter"), topOperatorAppearance: "none",
                                        showSubClauseButton: false, criteria: {}
                                    });
        let gasnApplyButton = isc.Button.create({
                                          ID: "gasnApplyButton", title: "Apply", autoFit: true, autoDraw: false,
                                          // @ts-ignore
                                          click: function() {
                                              let lgRecord = gasGrid.getSelectedRecord();
                                              if (lgRecord != null)
                                              {
                                                  // @ts-ignore
                                                  lgRecord.ds_criteria = gasNodeFilter.getCriteria();
                                              }
                                              gasnfWindow.hide();
                                          }
                                      });
        let gasnResetButton = isc.Button.create({
                                          ID: "gasnResetButton", title: "Reset", autoFit: true, autoDraw: false,
                                          // @ts-ignore
                                          click: function() {
                                              gasNodeFilter.clearCriteria();
                                          }
                                      });
        let gasnCancelButton = isc.IButton.create({
                                            ID: "gasnCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                                            // @ts-ignore
                                            click: function() {
                                                gasnfWindow.hide();
                                            }
                                        });
        let gasnButtonLayout = isc.HStack.create({
                                           ID: "gasnButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                                           autoDraw: false, membersMargin: 40,
                                           members: [ gasnApplyButton, gasnResetButton, gasnCancelButton ]
                                       });

        let gasnFormLayout = isc.VStack.create({
                                             ID: "gasnFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                             layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                             members:[ gasNodeFilter, gasnButtonLayout ]
                                         });
        let gasnfWindow = isc.Window.create({
                                        ID: "gasnfWindow", title: "Node Filter Builder Window", autoSize: true, autoCenter: true,
                                        isModal: true, showModalMask: true, autoDraw: false,
                                        items: [ gasnFormLayout ]
                                    });

        let gasRelFilter = isc.FilterBuilder.create({
                                         ID: "gasRelFilter", width: 500, height: 125, autoDraw: false,
                                         dataSource: windowAppContext.getAppPrefixDS("RelationshipFilter"), topOperatorAppearance: "none",
                                         showSubClauseButton: false, criteria: {}
                                     });
        let gasrApplyButton = isc.Button.create({
                                            ID: "gasrApplyButton", title: "Apply", autoFit: true, autoDraw: false,
                                            // @ts-ignore
                                            click: function() {
                                                let lgRecord = gasGrid.getSelectedRecord();
                                                if (lgRecord != null)
                                                {
                                                    // @ts-ignore
                                                    lgRecord.ds_criteria = gasRelFilter.getCriteria();
                                                }
                                                gasrfWindow.hide();
                                            }
                                        });
        let gasrResetButton = isc.Button.create({
                                            ID: "gasrResetButton", title: "Reset", autoFit: true, autoDraw: false,
                                            // @ts-ignore
                                            click: function() {
                                                gasRelFilter.clearCriteria();
                                            }
                                        });
        let gasrCancelButton = isc.IButton.create({
                                              ID: "gasrCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                                              // @ts-ignore
                                              click: function() {
                                                  gasrfWindow.hide();
                                              }
                                          });
        let gasrButtonLayout = isc.HStack.create({
                                             ID: "gasrButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                                             autoDraw: false, membersMargin: 40,
                                             members: [ gasrApplyButton, gasrResetButton, gasrCancelButton ]
                                         });

        let gasrFormLayout = isc.VStack.create({
                                           ID: "gasrFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                           layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                           members:[ gasRelFilter, gasrButtonLayout ]
                                       });
        let gasrfWindow = isc.Window.create({
                                        ID: "gasrfWindow", title: "Relationship Filter Builder Window", autoSize: true, autoCenter: true,
                                        isModal: true, showModalMask: true, autoDraw: false,
                                        items: [ gasrFormLayout ]
                                    });

        let gasAddButton = isc.ToolStripButton.create({
                                         ID: "gasAddButton", icon: "[SKIN]/actions/add.png", prompt: "Add Row", autoDraw: false, showDown: false,
                                         // @ts-ignore
                                         click: function() {
                                             let rowCount = gasGrid.data.length + 1;
                                             let isEven = rowCount % 2 == 0;
                                             if (isEven)
                                             {
                                                 let relIdentifier: string;
                                                 relIdentifier = windowAppContext.getGraphRelTypes().get(0);
                                                 gasGrid.data.add({object_type:"Relationship", object_identifier:relIdentifier, hop_count:0, edge_direction:"None", property_criteria:"Criteria"});
                                                 gasGrid.setValueMap("object_identifier", windowAppContext.getGraphRelTypes());
                                                 gasGrid.setEditorValueMap("object_identifier", windowAppContext.getGraphRelTypes());
                                             }
                                             else
                                             {
                                                 let nodeIdentifier: string;
                                                 nodeIdentifier = windowAppContext.getGraphNodeLabels().get(0);
                                                 gasGrid.data.add({object_type:"Node", object_identifier:nodeIdentifier, hop_count:0, edge_direction:"None", property_criteria:"Criteria"});
                                                 gasGrid.setValueMap("object_identifier", windowAppContext.getGraphNodeLabels());
                                                 gasGrid.setEditorValueMap("object_identifier", windowAppContext.getGraphNodeLabels());
                                             }
                                             gasGrid.startEditing(gasGrid.data.length-1);
                                         }
                                     });
        let gasDeleteButton = isc.ToolStripButton.create({
                                            ID: "gasDeleteButton", icon: "[SKIN]/actions/remove.png", prompt: "Delete Row", autoDraw: false, showDown: false,
                                            // @ts-ignore
                                            click: function() {
                                                let lgRecord = gasGrid.getSelectedRecord();
                                                if (lgRecord != null)
                                                    gasGrid.data.remove(lgRecord);
                                            }
                                        });
        let gasToolStrip = isc.ToolStrip.create({
                                      // @ts-ignore
                                      ID: "gasToolStrip", width: 500, height: 32, autoDraw: false,
                                      // @ts-ignore
                                      members: [gasAddButton, "separator", gasDeleteButton]
                                  });
        let gasGrid = isc.ListGrid.create({
                                      ID:"gasGrid", width: 500, height: 200, autoDraw: false, autoFetchData: false,
                                      canRemoveRecords: false, canSort: false, autoFitData: "horizontal",
                                      alternateRecordStyles: true, alternateFieldStyles: false,
                                      baseStyle: "alternateGridCell", showHeaderContextMenu: false,
                                      autoSaveEdits: true, canEdit: true, wrapCells: false,
                                      editEvent: "doubleClick", listEndEditAction: "next",
                                      // @ts-ignore
                                      cellClick: function (record, rowNum, colNum) {
                                                        if (colNum == 4)
                                                        {
                                                            gasGrid.endEditing();
                                                            let listGridRecord : isc.ListGridRecord;
                                                            listGridRecord = gasGrid.data.get(rowNum);
                                                            if (listGridRecord != null)
                                                            {
                                                                // @ts-ignore
                                                                if (listGridRecord.object_type === "Node")
                                                                {
                                                                    // @ts-ignore
                                                                    gasNodeFilter.setCriteria(listGridRecord.ds_criteria);
                                                                    gasnfWindow.show();
                                                                }
                                                                else
                                                                {
                                                                    // @ts-ignore
                                                                    gasRelFilter.setCriteria(listGridRecord.ds_criteria);
                                                                    gasrfWindow.show();
                                                                }
                                                            }
                                                            else
                                                                isc.say("listGridRecord is 'null' and cannot show filter form.");
                                                        }
                                                    },
                                      fields:[
                                          {name:"object_type", title:"Object", width:125, valueMap:["Node", "Relationship"],
                                              changed: function(form, item, value) {
                                                            const windowAppContext = (window as any)._appContext_;
                                                            let lgRecord = gasGrid.getSelectedRecord();
                                                            if (lgRecord != null)
                                                            {
                                                                // @ts-ignore
                                                                lgRecord.ds_criteria = {};
                                                            }
                                                            if (value === "Relationship")
                                                            {
                                                                gasGrid.setValueMap("object_identifier", windowAppContext.getGraphRelTypes());
                                                                gasGrid.setEditorValueMap("object_identifier", windowAppContext.getGraphRelTypes());
                                                            }
                                                            else
                                                            {
                                                                gasGrid.setValueMap("object_identifier", windowAppContext.getGraphNodeLabels());
                                                                gasGrid.setEditorValueMap("object_identifier", windowAppContext.getGraphNodeLabels());
                                                            }
                                              }},
                                          {name:"object_identifier", title:"Identifier", width:100, valueMap:windowAppContext.getGraphNodeLabels()},
                                          // @ts-ignore
                                          {name:"hop_count", title:"Hops", type:"integer", width:100, align:"center", editorType:"SpinnerItem", minValue:0, maxValue:20, defaultValue:0},
                                          {name:"edge_direction", title:"Direction", width:100, valueMap:["None", "Outbound", "Inbound"], defaultValue:"None"},
                                          {name:"property_criteria", title:"Criteria", type:"image", width:75, align:"center", canEdit:false, imageURLPrefix:"direction/", imageURLSuffix:".png", defaultValue:"Criteria"},
                                          {name:"ds_criteria", hidden:true, defaultValue:{}}
                                      ]
                      });
        let gasSearchButton = isc.Button.create({
                                       ID: "gasSearchButton", title: "Search", autoFit: true, autoDraw: false,
                                       // @ts-ignore
                                       click: function()
                                       {
                                           gasGrid.endEditing();
                                           RedisGraph.prototype.executeAppViewGraphSearch();
                                       }
                                   });
        let gasApplyButton = isc.Button.create({
                                      ID: "gasApplyButton", title: "Apply", autoFit: true, autoDraw: false,
                                      // @ts-ignore
                                      click: function() {
                                          gasGrid.endEditing();
                                          gasWindow.hide();
                                      }
                                  });
        let gasResetButton = isc.Button.create({
                                      ID: "gasResetButton", title: "Reset", autoFit: true, autoDraw: false,
                                      // @ts-ignore
                                      click: function() {
                                          while (! gasGrid.data.isEmpty())
                                            gasGrid.data.removeAt(0);
                                          gasGrid.setValueMap("object_identifier", windowAppContext.getGraphNodeLabels());
                                          gasGrid.setEditorValueMap("object_identifier", windowAppContext.getGraphNodeLabels());
                                      }
                                  });
        let gasCancelButton = isc.IButton.create({
                                        ID: "gasCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                                        // @ts-ignore
                                        click: function() {
                                            gasGrid.discardAllEdits();
                                            gasWindow.hide();
                                        }
                                    });
        let gasButtonLayout = isc.HStack.create({
                                       ID: "gasButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                                       autoDraw: false, layoutTopMargin: 30, membersMargin: 40,
                                       members: [ gasSearchButton, gasApplyButton, gasResetButton, gasCancelButton ]
                                   });

        let gasFormLayout = isc.VStack.create({
                                     ID: "gasFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                     layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30,
                                     members:[ gasToolStrip, gasGrid, gasButtonLayout ]
                                 });

        let gasWindow = isc.Window.create({
                                     ID: "gasWindow", title: "Graph Advanced Search Window", autoSize: true, autoCenter: true,
                                     isModal: false, showModalMask: false, autoDraw: false,
                                     items: [ gasFormLayout ]
                                 });

        let tsAdvancedSearch = isc.ToolStripButton.create({
                                         ID: "tsSearchAdvanced", icon: "[SKIN]/actions/ai-search-advanaced-icon.png", prompt: "Advanced Criteria Form", showDown: false, autoDraw: false,
                                          // @ts-ignore
                                          click: function()
                                          {
                                              gasWindow.show();
                                          }
                                     });
        let tsExecuteSearch = isc.ToolStripButton.create({
                                          ID: "tsSearchExecute", icon: "[SKIN]/pickers/search_picker.png", prompt: "Execute Search", showDown: false, autoDraw: false,
                                         // @ts-ignore
                                         click: function()
                                         {
                                             RedisGraph.prototype.executeAppViewGraphSearch();
                                         }
                                      });
        let tsClearSearch = isc.ToolStripButton.create({
                                       ID: "tsSearchClear", icon: "[SKIN]/pickers/clear_picker.png", prompt: "Clear Search Criteria", showDown: false, autoDraw: false,
                                       // @ts-ignore
                                       click: function()
                                       {
                                           const windowAppViewGrid = (window as any).appViewGrid;
                                           let appViewGrid: isc.ListGrid;
                                           appViewGrid = windowAppViewGrid;
                                           const windowSuggestForm = (window as any).suggestForm;
                                           let suggestForm: isc.DynamicForm;
                                           suggestForm = windowSuggestForm;
                                           searchForm.clearValues();
                                           suggestForm.clearValues();
                                           const windowGasGrid = (window as any).gasGrid;
                                           let gasGrid: isc.ListGrid;
                                           gasGrid = windowGasGrid;
                                           while (! gasGrid.data.isEmpty())
                                               gasGrid.data.removeAt(0);
                                           gasGrid.setValueMap("object_identifier", windowAppContext.getGraphNodeLabels());
                                           gasGrid.setEditorValueMap("object_identifier", windowAppContext.getGraphNodeLabels());
                                           gasRelFilter.clearCriteria();
                                           gasNodeFilter.clearCriteria();
                                           appViewGrid.invalidateCache();
                                           appViewGrid.filterData(RedisGraph.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()));
                                       }
                                     });
        let tsSearch = isc.ToolStrip.create({
                                        ID: "tsSearch", border: "0px", backgroundColor: "white", autoDraw: false,
                                         // @ts-ignore
                                         members: [tsAdvancedSearch, tsExecuteSearch, tsClearSearch]
                                     });
        let criteriaSearchLayout = isc.HStack.create({
                                 ID: "criteriaSearchLayout", width: "100%", align: "center",
                                 membersMargin: 2, layoutTopMargin: 10, layoutBottomMargin: 10,
                                 members:[ searchForm, tsSearch ]
        });

        // Auto suggestion search section
        let suggestForm = isc.DynamicForm.create({
                                                ID:"suggestForm", autoDraw: false,
                                                items: [{
                                                    name: "_suggest", title: "Suggestions", width: 300,
                                                    editorType: "ComboBoxItem", optionDataSource: "RG-SuggestList",
                                                    // @ts-ignore
                                                    pickListCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                                                    // @ts-ignore
                                                    keyPress : function(item: isc.FormItem, form: isc.DynamicForm, keyName: isc.KeyName, characterValue: number)
                                                            {
                                                                if (keyName == "Enter")
                                                                {
                                                                    const windowAppContext = (window as any)._appContext_;
                                                                    const windowAppViewGrid = (window as any).appViewGrid;
                                                                    let appViewGrid: isc.ListGrid;
                                                                    appViewGrid = windowAppViewGrid;
                                                                    let filterMap = new Map();
                                                                    filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
                                                                    filterMap.set("_dsStructure", windowAppContext.getDSStructure());
                                                                    filterMap.set("_appPrefix", windowAppContext.getPrefix());
                                                                    filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
                                                                    filterMap.set("_offset", windowAppContext.getCriteriaOffset());
                                                                    filterMap.set("_limit", 10);
                                                                    let suggestTerm = form.getValue("_suggest");
                                                                    filterMap.set("_search", suggestTerm);
                                                                    let simpleCriteria = {};
                                                                    filterMap.forEach((value, key) => {
                                                                        // @ts-ignore
                                                                        simpleCriteria[key] = value
                                                                    });
                                                                    appViewGrid.filterData(simpleCriteria);
                                                                }
                                                                return true;
                                                            }
                                                    }]
                                                });
        let tsClearSuggestion = isc.ToolStripButton.create({
                                       ID: "tsClearSuggestion", icon: "[SKIN]/pickers/clear_picker.png", prompt: "Clear Search Suggestion", showDown: false, autoDraw: false,
                                       // @ts-ignore
                                       click: function()
                                       {
                                           const windowAppViewGrid = (window as any).appViewGrid;
                                           let appViewGrid: isc.ListGrid;
                                           appViewGrid = windowAppViewGrid;
                                           const windowSuggestForm = (window as any).suggestForm;
                                           let suggestForm: isc.DynamicForm;
                                           suggestForm = windowSuggestForm;
                                           searchForm.clearValues();
                                           suggestForm.clearValues();
                                           const windowGasGrid = (window as any).gasGrid;
                                           let gasGrid: isc.ListGrid;
                                           gasGrid = windowGasGrid;
                                           while (! gasGrid.data.isEmpty())
                                               gasGrid.data.removeAt(0);
                                           gasGrid.setValueMap("object_identifier", windowAppContext.getGraphNodeLabels());
                                           gasGrid.setEditorValueMap("object_identifier", windowAppContext.getGraphNodeLabels());
                                           gasRelFilter.clearCriteria();
                                           gasNodeFilter.clearCriteria();
                                           appViewGrid.invalidateCache();
                                           appViewGrid.filterData(RedisGraph.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()));
                                       }
                                   });
        let tsSuggest = isc.ToolStrip.create({
                                                 ID: "tsSuggest", border: "0px", backgroundColor: "white", autoDraw: false,
                                                members: [tsClearSuggestion]
                                            });
        let suggestedSearchLayout = isc.HStack.create({
                                          ID: "suggestedSearchLayout", width: "100%", align: "center",
                                          membersMargin: 2, layoutTopMargin: 10, layoutBottomMargin: 10,
                                         members:[ suggestForm, tsSuggest ]
                                         });
        let searchOptionsStack = isc.SectionStack.create({
                            ID: "searchOptionsStack", visibilityMode: "mutex", width: "100%", headerHeight: 23, autoDraw: false,
                            sections: [
                                {title: "Criteria Search", expanded: true, canCollapse: true, items: [ criteriaSearchLayout ]},
                                {title: "Suggested Search", expanded: false, canCollapse: true, items: [ suggestedSearchLayout ]}
                            ]
                        });

        return searchOptionsStack;
    }

    // Main grid: delete callback that generates a notification message.
    private deleteSelectedAppViewGridRow(aResponse: string): void
    {
        if (aResponse == "OK")
        {
            const windowAppViewGrid = (window as any).appViewGrid;
            let appViewGrid: isc.ListGrid;
            appViewGrid = windowAppViewGrid;
            if (appViewGrid != null)
            {
                appViewGrid.removeSelectedData();
                isc.Notify.addMessage("Row deleted", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
            }
        }
    }

    // File->Applications->Manage (Grid): row delete callback that generates a notification message.
    private deleteSelectedApplicationsGridRow(aResponse: string): void
    {
        if (aResponse == "OK")
        {
            const windowApplicationsGrid = (window as any).applicationsGrid;
            let applicationsGrid: isc.ListGrid;
            applicationsGrid = windowApplicationsGrid;
            if (applicationsGrid != null)
            {
                applicationsGrid.removeSelectedData();
                isc.Notify.addMessage("Row deleted", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
            }
        }
    }

    // File->Documents->Manage (Grid): row delete callback that generates a notification message.
    private deleteSelectedDocumentsGridRow(aResponse: string): void
    {
        if (aResponse == "OK")
        {
            const windowDocumentsGrid = (window as any).documentsGrid;
            let documentsGrid: isc.ListGrid;
            documentsGrid = windowDocumentsGrid;
            if (documentsGrid != null)
            {
                documentsGrid.removeSelectedData();
                isc.Notify.addMessage("Row deleted", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
            }
        }
    }

    // Graph relationship grid: delete callback that generates a notification message.
    private deleteSelectedGraphGridRelRow(aResponse: string): void
    {
        if (aResponse == "OK")
        {
            const windowGraphGridRel = (window as any).graphGridRel;
            let graphGridRel: isc.ListGrid;
            graphGridRel = windowGraphGridRel;
            if (graphGridRel != null)
            {
                graphGridRel.removeSelectedData();
                isc.Notify.addMessage("Row deleted", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
            }
        }
    }

    // Main and relationship grid: form save callback that generates a notification message.
    private updateCallback(aDSResponse: isc.DSResponse, aData: any, aDSRequest: isc.DSRequest): void
    {
        isc.Notify.addMessage("Form saved", null, null, {
            canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
        });

        const windowAppContext = (window as any)._appContext_;
        if (windowAppContext.isGraphEnabled())
        {
            let scDataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
            const windowGraphGridRel = (window as any).graphGridRel;
            let graphGridRel: isc.ListGrid;
            graphGridRel = windowGraphGridRel;
            let primaryKeyField = scDataSource.getPrimaryKeyField();
            let keyName = primaryKeyField.name;
            let simpleCriteria = {};
            // @ts-ignore
            simpleCriteria[keyName] = appViewForm.getValue(keyName);
            graphGridRel.invalidateCache();
            graphGridRel.filterData(simpleCriteria);
        }
    }

    // Database: flush all data from database
    private flushDatabase(aResponse: string): void
    {
        if (aResponse == "OK")
        {
            const windowAppContext = (window as any)._appContext_;
            const windowRedisDBInfoWindow = (window as any).redisDBInfoWindow;
            let redisDBInfoWindow: isc.Window;
            redisDBInfoWindow = windowRedisDBInfoWindow;
            const windowRedisDBInfoForm = (window as any).redisDBInfoForm;
            let redisDBInfoForm: isc.DynamicForm;
            redisDBInfoForm = windowRedisDBInfoForm;
            if ((redisDBInfoWindow != null) && (redisDBInfoForm != null))
            {
                // We use the remove operation to trigger the database flush operation on the server side
                windowAppContext.assignFormContext(redisDBInfoForm);
                redisDBInfoForm.saveData();
                redisDBInfoWindow.hide();
                isc.Notify.addMessage("Database flush initiated", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
                // Allow time for the index rebuild and notification to complete before schema refresh
                setTimeout(() => { RedisGraph.prototype.executeAppViewGridFetch(); }, 2000);
            }
        }
    }

    private fieldPrefix(aFieldName: string): string
    {
        let fieldName = aFieldName.toLowerCase();
        let offset = fieldName.indexOf("_");
        if (offset < 2)
            return fieldName;
        else
            return fieldName.substring(0, offset);
    }

    private uniqueFormNames(aDS: isc.DataSource): Map<string,string>
    {
        let formName: string;
        let fieldPrefix: string;
        let fieldLookup: string;
        let formNames = new Map<string,string>();
        formNames.set("common", "Common");

        // @ts-ignore
        for (let fieldName of aDS.getFieldNames(false))
        {
            fieldPrefix = this.fieldPrefix(fieldName);
            if (! fieldPrefix.startsWith("ras"))
            {
                fieldLookup = formNames.get(fieldPrefix);
                if (fieldLookup == undefined)
                {
                    formName = fieldPrefix.charAt(0).toUpperCase() + fieldPrefix.slice(1);
                    formNames.set(fieldPrefix, formName);
                }
            }
        }

        return formNames;
    }

    private createGraphRelationshipFormLayout(): isc.VStack
    {
        const windowAppContext = (window as any)._appContext_;

        let graphEdgeFormLayout = isc.VStack.create({
                                        ID: "graphEdgeFormLayout", width: "100%", align: "center", autoDraw: false, membersMargin: 2
                                    });
        let graphRelForm = isc.DynamicForm.create({
                                      ID: "graphRelForm", width: 300, numCols:2, autoDraw: false, dataSource: windowAppContext.getAppViewRelDS()
                                  });
        // @ts-ignore
        graphEdgeFormLayout.addMember(graphRelForm);
        let graphEdgeVertexForm = isc.DynamicForm.create({
                                     ID: "graphEdgeVertexForm", width: 300, numCols:2, autoDraw: false,
                                     items: [{
                                         name: "_suggest", title: "End Node", editorType: "ComboBoxItem", optionDataSource: "RG-SuggestList",  required: true,
                                         // @ts-ignore
                                         pickListCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                                         // @ts-ignore
                                         keyPress : function(item: isc.FormItem, form: isc.DynamicForm, keyName: isc.KeyName, characterValue: number)
                                         {
                                             if (keyName == "Enter")
                                             {
                                                 const windowGraphRelForm = (window as any).graphRelForm;
                                                 let graphRelForm: isc.DynamicForm;
                                                 graphRelForm = windowGraphRelForm;
                                                 graphRelForm.setValue("common_vertex_name", graphEdgeVertexForm.getValue("_suggest"));
                                             }
                                             return true;
                                         }
                                     }]
                          });
        // @ts-ignore
        graphEdgeFormLayout.addMember(graphEdgeVertexForm);
        let fiVertexName = graphRelForm.getItem("common_vertex_name");
        fiVertexName.visible = false;

        return graphEdgeFormLayout;
    }

    /* This function will decompose data source of fields into a collection of
     * form groups to aid the user with updating their vertex/node data.  This
     * logic assumes that fields follow a naming convention where common fields
     * are prefixed with "common_" and all others with their vertex labels. */
    private createGraphNodeFormLayout(aDS: isc.DataSource): isc.VStack
    {
        let uiField: any;
        let formName: string;
        let avProperties: any;
        let dynamicForm: isc.DynamicForm;
        let dsField: isc.DataSourceField;
        const windowAppContext = (window as any)._appContext_;

        let graphNodeFormLayout = isc.VStack.create({
                                ID: "graphNodeFormLayout", width: "100%", align: "center", autoDraw: false, membersMargin: 5
                            });
        let formNames = this.uniqueFormNames(aDS);
        formNames.forEach((value, key) => {
            // @ts-ignore
            let uiFields = [];
            formName = key + "AppViewForm";
            avProperties = {ID: formName, width: 500, numCols:2, colWidths: [150, "*"], autoDraw: false, isGroup: true, groupTitle: value};
            for (let fieldName of aDS.getFieldNames(false))
            {
                if (fieldName.startsWith(key))
                {
                    dsField = aDS.getField(fieldName);
                    uiField = {};
                    // @ts-ignore
                    uiField["name"] = dsField.name;
                    // @ts-ignore
                    uiField["title"] = dsField.title;
                    if (fieldName.startsWith("common_"))
                    {
                        // @ts-ignore
                        uiField["required"] = "true";
                        if (fieldName == "common_vertex_label")
                        {
                            // @ts-ignore
                            uiField["hint"] = "<nobr>Node label</nobr>";
                            // @ts-ignore
                            uiField["editorType"] = "SelectItem";
                            uiField["changed"] = "RedisGraph.prototype.showGraphNodeForm(value);";
                        }
                        else if (fieldName == "common_name")
                        {
                            // @ts-ignore
                            uiField["hint"] = "<nobr>Node name</nobr>";
                        }
                    }

                    // @ts-ignore
                    uiFields.add(uiField);
                }
            }
            // @ts-ignore
            avProperties["fields"] = uiFields;
            dynamicForm = isc.DynamicForm.create(avProperties);
            // @ts-ignore
            let selectNodeLabels = [];
            formNames.forEach((value, key) => {
                if ((! key.startsWith("common")) && (! key.startsWith("ras")))
                {
                    // @ts-ignore
                    selectNodeLabels.add(value);
                }
            });
            // @ts-ignore
            dynamicForm.setValueMap("common_vertex_label", selectNodeLabels);
            // @ts-ignore
            graphNodeFormLayout.addMember(dynamicForm);
            windowAppContext.add(formName, dynamicForm);
        });

        return graphNodeFormLayout;
    }

    private graphNodeFormsClearValues(): void
    {
        const windowAppContext = (window as any)._appContext_;
        const windowAppViewForm = (window as any).appViewForm;
        let dynamicForm: isc.DynamicForm;

        let formName: string;
        let scDataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
        let formNames = this.uniqueFormNames(scDataSource);
        formNames.forEach((value, key) =>
                          {
                              formName = key + "AppViewForm";
                              dynamicForm = windowAppContext.get(formName);
                              dynamicForm.clearValues();
                              dynamicForm.clearErrors(false);
                              dynamicForm.show();
                          });
    }

    private graphNodeFormsIsValid(aShowValid: boolean): boolean
    {
        const windowAppContext = (window as any)._appContext_;
        let dynamicForm: isc.DynamicForm;

        dynamicForm = windowAppContext.get("commonAppViewForm");
        if (aShowValid)
            dynamicForm.validate(false);
        else if (! dynamicForm.valuesAreValid(false, false))
            return false;

        return true;
    }

    private appViewToGraphNodeForms(): void
    {
        const windowAppContext = (window as any)._appContext_;
        const windowAppViewForm = (window as any).appViewForm;
        let appViewForm: isc.DynamicForm;
        appViewForm = windowAppViewForm;
        let dynamicForm: isc.DynamicForm;

        let formName: string;
        let scDataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
        let formNames = this.uniqueFormNames(scDataSource);
        formNames.forEach((value, key) =>
                          {
                              formName = key + "AppViewForm";
                              dynamicForm = windowAppContext.get(formName);
                              dynamicForm.clearValues();
                              for (let formItem of dynamicForm.fields)
                                  dynamicForm.setValue(formItem.name, appViewForm.getValue(formItem.name));
                          });
    }

    private graphNodesToAppViewForm(): void
    {
        const windowAppContext = (window as any)._appContext_;
        const windowAppViewForm = (window as any).appViewForm;
        let appViewForm: isc.DynamicForm;
        appViewForm = windowAppViewForm;
        let dynamicForm: isc.DynamicForm;

        let formName: string;
        let scDataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
        let formNames = this.uniqueFormNames(scDataSource);
        formNames.forEach((value, key) =>
                          {
                              formName = key + "AppViewForm";
                              dynamicForm = windowAppContext.get(formName);
                              for (let formItem of dynamicForm.fields)
                                  appViewForm.setValue(formItem.name, dynamicForm.getValue(formItem.name));
                          });
    }

    private showGraphNodeForm(aLabelName: string): void
    {
        if (aLabelName != undefined)
        {
            const windowAppContext = (window as any)._appContext_;
            let dynamicForm: isc.DynamicForm;

            let formName: string;
            let formLabelPrefix = aLabelName.toLowerCase();
            let selectedFormName = formLabelPrefix + "AppViewForm";
            let scDataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
            let formNames = this.uniqueFormNames(scDataSource);
            formNames.forEach((value, key) =>
                  {
                      formName = key + "AppViewForm";
                      dynamicForm = windowAppContext.get(formName);
                      if (formName === selectedFormName)
                          dynamicForm.show();
                      else if (formName != "commonAppViewForm")
                          dynamicForm.hide();
                  });
        }
    }

    private graphVisualizationOptionsToURL(isDownload: boolean): string
    {
        const windowAppContext = (window as any)._appContext_;
        const windowGVOptionsForm = (window as any).gvOptionsForm;
        let gvOptionsForm: isc.DynamicForm;
        gvOptionsForm = windowGVOptionsForm;

        let gvURL = windowAppContext.getGraphVisualizationURL() + "?";
        gvURL += "is_modeler=false&";
        gvURL += "is_hierarchical=" + gvOptionsForm.getValue("is_hierarchical") + "&";
        gvURL += "is_matched=" + gvOptionsForm.getValue("is_matched") + "&";
        gvURL += "node_shape=" + gvOptionsForm.getValue("node_shape") + "&";
        gvURL += "node_color=" + gvOptionsForm.getValue("node_color") + "&";
        gvURL += "edge_arrow=" + gvOptionsForm.getValue("edge_arrow") + "&";
        gvURL += "edge_color=" + gvOptionsForm.getValue("edge_color") + "&";
        gvURL += "match_color=" + gvOptionsForm.getValue("match_color");
        if (isDownload)
            gvURL += "&is_download=true";

        let gvEncodedURL = gvURL.replace(/#/g, "%23");

        isc.logWarn("gvEncodedURL = " + gvEncodedURL);

        return gvEncodedURL;
    }

    // Create the ToolStrip component with command buttons
    private createCommandToolStrip(): isc.ToolStrip
    {
        const windowAppContext = (window as any)._appContext_;

        // ToolStrip "File" Menu Item - Redis Data->Information form
        let redisDBInfoForm = isc.DynamicForm.create({
                                     ID: "redisDBInfoForm", width: 400, height: 400, autoDraw: false, dataSource: "RG-Database", autoFetchData:false, canEdit: false
                                 });
        let redisDBInfoLayout = isc.VStack.create({
                                      ID: "redisDBInfoLayout", width: "100%", height: "100%", autoDraw: false, layoutAlign: "center",
                                      layoutTopMargin: 20, layoutBottomMargin: 20, layoutLeftMargin: 20, layoutRightMargin: 20,
                                      members:[ redisDBInfoForm ]
                                  });
        let redisDBInfoWindow = isc.Window.create({
                                      ID: "redisDBInfoWindow", title: "Redis DB Info Window", width: 410, height: 520, autoCenter: true,
                                      isModal: false, showModalMask: false, canDragResize: true, autoDraw: false,
                                      items: [ redisDBInfoLayout ]
                                  });

        // ToolStrip "File" menu
        let fileMenu = isc.Menu.create({
                               ID: "fileMenu", showShadow: true, shadowDepth: 10, autoDraw: false,
                               data: [
                                   {title: "Redis Data ...", icon: "[SKIN]/actions/ai-save-icon.png",
                                       submenu: [
                                           {title: "Information", icon: "[SKIN]/actions/ai-redis-connect-icon.png", enabled: true, checked: false, click: function() {
                                                   const windowAppContext = (window as any)._appContext_;
                                                   windowAppContext.assignFormContext(redisDBInfoForm);
                                                   redisDBInfoForm.fetchData(RedisGraph.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()), function () {
                                                       redisDBInfoWindow.show();
                                                   });
                                               }},
                                           {title: "Flush DB", icon: "[SKIN]/actions/ai-commands-delete-icon.png", enabled: true, click: function() {
                                                   const windowAppContext = (window as any)._appContext_;
                                                   windowAppContext.assignFormContext(redisDBInfoForm);
                                                   redisDBInfoForm.fetchData(RedisGraph.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()), function () {
                                                       redisDBInfoWindow.show();
                                                       // @ts-ignore  - You needed to look at the compiled JavaScript to get the callback name correct
                                                       isc.confirm("Are you sure you want to flush all data?", "RedisGraph.prototype.flushDatabase(value ? 'OK' : 'Cancel')");
                                                   });
                                               }}
                                       ]},
                                   {isSeparator: true},
                                   {title: "Export Data ...", icon: "[SKIN]/actions/ai-export-icon.png", enabled: true,
                                       submenu: [
                                           {title: "Grid as PDF", icon: "[SKIN]/actions/ai-export-grid-pdf-icon.png", click: function() {
                                                   const windowAppViewGrid = (window as any).appViewGrid;
                                                   let appViewGrid: isc.ListGrid;
                                                   appViewGrid = windowAppViewGrid;
                                                   // @ts-ignore
                                                   isc.Canvas.showPrintPreview(appViewGrid);
                                               }},
                                           {title: "Grid as CSV", icon: "[SKIN]/actions/ai-export-grid-csv-icon.png", click: function() {
                                                   const windowAppContext = (window as any)._appContext_;
                                                   RedisGraph.prototype.executeAppViewGridExport("grid_export_by_criteria_csv", windowAppContext.getGridCSVHeader(), 100);
                                               }},
                                           {title: "Grid as JSON", icon: "[SKIN]/actions/ai-export-grid-json-icon.png", click: function() {
                                                   RedisGraph.prototype.executeAppViewGridExport("grid_export_by_criteria_json", "json", 100);
                                               }},
                                           {title: "Schema as XML", icon: "[SKIN]/actions/ai-export-schema-xml-icon.png", click: function() {
                                                   RedisGraph.prototype.executeAppViewGridExport("schema_export_xml", "xml", 100);
                                               }},
                                           {title: "Commands as TXT", icon: "[SKIN]/actions/ai-commands-icon.png", click: function() {
                                                   RedisGraph.prototype.executeAppViewGridExport("command_export_txt", "txt", 100);
                                               }}
                                       ]}
                               ]
                           });
        let fileMenuButton = isc.ToolStripMenuButton.create({
                                        ID: "fileMenuButton", title: "File", autoDraw: false, menu: fileMenu
                                    });

        // ToolStrip "Schema" button and related grid and forms
        let tsSchemaButton: isc.ToolStripButton;
        let scDataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
        let nodeSchemaGrid = isc.ListGrid.create({
                             ID:"nodeSchemaGrid", width: 710, height: 428, autoDraw: false, dataSource: "RG-NodeSchemaGrid",
                             initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                             autoFetchData: false, canEdit: true, canSort: false, showHeaderContextMenu: false, editEvent: "click",
                             alternateRecordStyles:true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
                             listEndEditAction: "next", autoSaveEdits: false,
                             // @ts-ignore
                             getCellCSSText: function (record, rowNum, colNum) {
                                 if (colNum == 0)
                                     return "font-weight:bold; color:#000000;";
                             }
                         });
        let relSchemaGrid = isc.ListGrid.create({
                             ID:"relSchemaGrid", width: 710, height: 428, autoDraw: false, dataSource: "RG-RelSchemaGrid",
                             initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                             autoFetchData: false, canEdit: true, canSort: false, showHeaderContextMenu: false, editEvent: "click",
                             alternateRecordStyles:true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
                             listEndEditAction: "next", autoSaveEdits: false,
                             // @ts-ignore
                             getCellCSSText: function (record, rowNum, colNum) {
                                 if (colNum == 0)
                                     return "font-weight:bold; color:#000000;";
                             }
                         });
        let tsGraphSchemaTab = isc.TabSet.create({
                           ID: "tsGraphSchemaTab", tabBarPosition: "top", width: 725, height: 475, autoDraw: false,
                           tabs: [
                                   {title: "Node Schema", pane: nodeSchemaGrid },
                                   {title: "Relationship Schema", pane: relSchemaGrid }
                                 ]
                       });

        let sgApplyButton = isc.Button.create({
                                      ID: "sgApplyButton", title: "Update", autoFit: true, autoDraw: false,
                                      // @ts-ignore
                                      click: function() {
                                          const windowGraphSchemaTab = (window as any).tsGraphSchemaTab;
                                          let tsGraphSchemaTab: isc.TabSet;
                                          tsGraphSchemaTab = windowGraphSchemaTab;
                                          let selectedTabOffset = tsGraphSchemaTab.getSelectedTabNumber();
                                          if (selectedTabOffset == 0)
                                          {
                                              nodeSchemaGrid.saveAllEdits();
                                              isc.Notify.addMessage("Node updates saved", null, null, {
                                                  canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                                              });
                                          }
                                          else
                                          {
                                              relSchemaGrid.saveAllEdits();
                                              isc.Notify.addMessage("Relationship updates saved", null, null, {
                                                  canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                                              });
                                          }

                                      }
                                  });
        let sgDiscardButton = isc.Button.create({
                                        ID: "sgDiscardButton", title: "Discard", autoFit: true, autoDraw: false,
                                        // @ts-ignore
                                        click: function() {
                                            nodeSchemaGrid.discardAllEdits();
                                            relSchemaGrid.discardAllEdits();
                                            isc.Notify.addMessage("Updates discarded", null, null, {
                                                canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                                            });
                                        }
                                    });
        let sgCloseButton = isc.IButton.create({
                                       ID: "sgCloseButton", title: "Close", autoFit: true, autoDraw: false,
                                       // @ts-ignore
                                       click: function() {
                                           sgWindow.hide();
                                       }
                                   });
        let sgButtonLayout = isc.HStack.create({
                                       ID: "sgButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                                       autoDraw: false, membersMargin: 40,
                                       members: [ sgApplyButton, sgDiscardButton, sgCloseButton ]
                                   });
        let sgFormLayout = isc.VStack.create({
                                         ID: "sgFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                         layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                         members:[ tsGraphSchemaTab, sgButtonLayout ]
                                     });
        let sgWindow = isc.Window.create({
                                         ID: "sgWindow", title: "Schema Editor Window", autoSize: true, autoCenter: true,
                                         isModal: true, showModalMask: true, autoDraw: false,
                                         items: [ sgFormLayout ]
                                     });
        tsSchemaButton = isc.ToolStripButton.create({
                                    ID: "tsSchemaButton", icon: "[SKIN]/actions/ai-schema-icon.png", prompt: "Schema Form", showDown: false, autoDraw: false,
                                    // @ts-ignore
                                    click: function()
                                    {
                                        nodeSchemaGrid.invalidateCache();
                                        nodeSchemaGrid.filterData(RedisGraph.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                                                              function (aDSResponse: isc.DSResponse, aData: any, aDSRequest: isc.DSRequest) {
                                                                  let gridData : isc.List;
                                                                  gridData = aData;
                                                                  const windowAppViewGrid = (window as any).appViewGrid;
                                                                  let appViewGrid: isc.ListGrid;
                                                                  appViewGrid = windowAppViewGrid;
                                                                  let isVisible : boolean;
                                                                  let gridRecord : isc.ListGridRecord;
                                                                  for (let recOffset = 0; recOffset < gridData.getLength(); recOffset++)
                                                                  {
                                                                      isVisible = false;
                                                                      gridRecord = gridData.get(recOffset);
                                                                      let listGridFields = appViewGrid.getFields();
                                                                      for (let lgField of listGridFields)
                                                                      {
                                                                          // @ts-ignore
                                                                          if (gridRecord.item_name == lgField.name)
                                                                          {
                                                                              isVisible = true;
                                                                              // @ts-ignore
                                                                              gridRecord.item_title = lgField.title;
                                                                          }
                                                                      }
                                                                      // @ts-ignore
                                                                      gridRecord.isVisible = isVisible;
                                                                  }
                                                                  relSchemaGrid.invalidateCache();
                                                                  relSchemaGrid.filterData(RedisGraph.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()));
                                                              });
                                        sgWindow.show();
                                    }
                                });

        // ToolStrip "Add, Edit and Delete" buttons and related forms
        let avfWindow: isc.Window;
        let appViewForm: isc.DynamicForm;
        let appViewFormLayout: isc.VStack;
        appViewForm = isc.DynamicForm.create({
                                 ID: "appViewForm", autoDraw: false, dataSource: windowAppContext.getAppViewDS()
                             });
        let avfSaveButton = isc.Button.create({
                                  ID: "avfSaveButton", title: "Save", autoFit: true, autoDraw: false,
                                  // @ts-ignore
                                  click: function() {
                                      if (RedisGraph.prototype.graphNodeFormsIsValid(false))
                                      {
                                          RedisGraph.prototype.graphNodesToAppViewForm();
                                          windowAppContext.assignFormContext(appViewForm);
                                          // @ts-ignore
                                          appViewForm.saveData("RedisGraph.prototype.updateCallback(dsResponse,data,dsRequest)");
                                          avfWindow.hide();
                                      }
                                      else
                                          RedisGraph.prototype.graphNodeFormsIsValid(true);
                                  }
                              });
        let avfCancelButton = isc.IButton.create({
                                 ID: "avfCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                                 // @ts-ignore
                                 click: function() {
                                     avfWindow.hide();
                                 }
                             });
        let avfButtonLayout = isc.HStack.create({
                                ID: "avfButtonLayout", width: "100%", height: 24, layoutAlign: "center", autoDraw: false, layoutLeftMargin: 175, membersMargin: 40,
                                members: [ avfSaveButton, avfCancelButton ]
                            });

        // Graph node form
        let graphNodeFormLayout = this.createGraphNodeFormLayout(scDataSource);
        appViewFormLayout = isc.VStack.create({
                                  ID: "appViewFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 10,
                                  layoutBottomMargin: 10, layoutLeftMargin: 45, layoutRightMargin: 10, membersMargin: 20,
                                  members:[ graphNodeFormLayout, avfButtonLayout ]
                              });

        // Graph relationship form
        let graphEdgeFormLayout = this.createGraphRelationshipFormLayout();
        let grAddButton = isc.ToolStripButton.create({
                                 ID: "grAddButton", icon: "[SKIN]/actions/add.png", prompt: "Add Row", autoDraw: false, showDown: false,
                                 // @ts-ignore
                                 click: function() {
                                     const windowGraphRelForm = (window as any).graphRelForm;
                                     let graphRelForm: isc.DynamicForm;
                                     graphRelForm = windowGraphRelForm;
                                     graphRelForm.hideItem("common_edge_direction");
                                     graphRelForm.clearValues();
                                     graphRelForm.clearErrors(false);
                                     graphRelForm.editNewRecord();
                                     graphRelForm.getItem("common_type").enable();
                                     const windowGraphEdgeVertexForm = (window as any).graphEdgeVertexForm;
                                     let graphEdgeVertexForm: isc.DynamicForm;
                                     graphEdgeVertexForm = windowGraphEdgeVertexForm;
                                     graphEdgeVertexForm.clearValues();
                                     graphEdgeVertexForm.getItem("_suggest").enable();
                                     grfWindow.setTitle("Edit Relationship Form Window");
                                     grfWindow.show();
                                 }
                             });
        let grEditButton = isc.ToolStripButton.create({
                                  ID: "grEditButton", icon: "[SKIN]/actions/ai-edit-icon.png", prompt: "Edit Row", autoDraw: false, showDown: false,
                                  // @ts-ignore
                                  click: function() {
                                      const windowGraphGridRel = (window as any).graphGridRel;
                                      let graphGridRel: isc.ListGrid;
                                      graphGridRel = windowGraphGridRel;
                                      if (graphGridRel != null)
                                      {
                                          let lgRecord = graphGridRel.getSelectedRecord();
                                          if (lgRecord != null)
                                          {
                                              const windowGraphRelForm = (window as any).graphRelForm;
                                              let graphRelForm: isc.DynamicForm;
                                              graphRelForm = windowGraphRelForm;
                                              graphRelForm.hideItem("common_edge_direction");
                                              graphRelForm.clearValues();
                                              graphRelForm.clearErrors(false);
                                              graphRelForm.editSelectedData(graphGridRel);
                                              graphRelForm.getItem("common_type").disable();
                                              grfWindow.setTitle("Edit Relationship Form Window");
                                              const windowGraphEdgeVertexForm = (window as any).graphEdgeVertexForm;
                                              let graphEdgeVertexForm: isc.DynamicForm;
                                              graphEdgeVertexForm = windowGraphEdgeVertexForm;
                                              graphEdgeVertexForm.setValue("_suggest", graphRelForm.getValue("common_vertex_name"));
                                              graphEdgeVertexForm.getItem("_suggest").disable();
                                              grfWindow.show();
                                          }
                                          else
                                              isc.say("You must select a row on the grid to edit.");
                                      }
                                  }
                              });
        let grDeleteButton = isc.ToolStripButton.create({
                                ID: "grDeleteButton", icon: "[SKIN]/actions/remove.png", prompt: "Delete Row", autoDraw: false, showDown: false,
                                // @ts-ignore
                                click: function() {
                                    const windowGraphGridRel = (window as any).graphGridRel;
                                    let graphGridRel: isc.ListGrid;
                                    graphGridRel = windowGraphGridRel;
                                    let lgRecord = graphGridRel.getSelectedRecord();
                                    if (lgRecord != null)
                                    {
                                        // @ts-ignore  - You needed to look at the compiled JavaScript to get the callback name correct
                                        isc.confirm("Proceed with row deletion operation?", "RedisGraph.prototype.deleteSelectedGraphGridRelRow(value ? 'OK' : 'Cancel')");
                                    }
                                    else
                                        isc.say("You must select a row on the grid to remove.");
                                }
                            });
        let grfSaveButton = isc.Button.create({
                              ID: "grfSaveButton", title: "Save", autoFit: true, autoDraw: false,
                              // @ts-ignore
                              click: function() {
                                  const windowGraphRelForm = (window as any).graphRelForm;
                                  let graphRelForm: isc.DynamicForm;
                                  graphRelForm = windowGraphRelForm;
                                  if (graphRelForm.valuesAreValid(false, false))
                                  {
                                      const windowAppViewForm = (window as any).appViewForm;
                                      let appViewForm: isc.DynamicForm;
                                      appViewForm = windowAppViewForm;
                                      graphRelForm.setValue("common_vertex_src_id", appViewForm.getValue("common_id"));
                                      const windowGraphEdgeVertexForm = (window as any).graphEdgeVertexForm;
                                      let graphEdgeVertexForm: isc.DynamicForm;
                                      graphEdgeVertexForm = windowGraphEdgeVertexForm;
                                      graphRelForm.setValue("common_vertex_name", graphEdgeVertexForm.getValue("_suggest"));
                                      windowAppContext.assignFormContext(graphRelForm);
                                      // @ts-ignore
                                      graphRelForm.saveData("RedisGraph.prototype.updateCallback(dsResponse,data,dsRequest)");
                                      grfWindow.hide();
                                  }
                                  else
                                      graphRelForm.validate(false);
                              }
                          });
        let grfCancelButton = isc.IButton.create({
                             ID: "grfCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                             // @ts-ignore
                             click: function() {
                                 grfWindow.hide();
                             }
                         });
        let grfButtonLayout = isc.HStack.create({
                            ID: "grfButtonLayout", width: "100%", height: 24, layoutAlign: "center", autoDraw: false, layoutLeftMargin: 30, membersMargin: 40,
                            members: [ grfSaveButton, grfCancelButton ]
                        });
        let graphRelFormLayout = isc.VStack.create({
                              ID: "graphRelFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                              layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                              members:[ graphEdgeFormLayout, grfButtonLayout ]
                          });
        let grfWindow = isc.Window.create({
                              ID: "grfWindow", title: "Graph Relationship Form Window", autoSize: true, canDragResize: true,
                              autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
                              items: [ graphRelFormLayout ]
                          });
        let graphToolStrip = isc.ToolStrip.create({
                             // @ts-ignore
                             ID: "graphToolStrip", width: "100%", height: 32, autoDraw: false,
                             // @ts-ignore
                             members: [grAddButton, "separator", grEditButton, "separator", grDeleteButton]
                         });
        let graphGridRelOut = isc.ListGrid.create({
                            ID: "graphGridRel", dataSource: windowAppContext.getAppViewRelOutDS(),
                            autoDraw: false, width: 588, height: 473, autoFetchData: false, showFilterEditor: false,
                            allowFilterOperators: false, filterOnKeypress: false, useAdvancedFieldPicker: false,
                            canEditTitles: false, canEdit: false, leaveScrollbarGap: false
                        });
        let tsGridLayout = isc.VStack.create({
                              ID: "tsGridLayout", width: "100%", autoDraw: false, layoutTopMargin: 10,
                              members:[ graphToolStrip, graphGridRelOut ]
                          });
        let tsGraphTab = isc.TabSet.create({
                            ID: "tsGraphTab", tabBarPosition: "top", width: 600, height: 575, autoDraw: false,
                            tabs: [
                                {title: "Node", pane: appViewFormLayout },
                                {title: "Relationships", pane: tsGridLayout,
                                    // @ts-ignore
                                    tabSelected : function (tabSet, tabNum, tabPane, ID, tab)
                                                    {
                                                        if (tabNum == 1)
                                                        {
                                                            let primaryKeyField = scDataSource.getPrimaryKeyField();
                                                            let keyName = primaryKeyField.name;
                                                            let simpleCriteria = {};
                                                            // @ts-ignore
                                                            simpleCriteria[keyName] = appViewForm.getValue(keyName);
                                                            graphGridRelOut.filterData(simpleCriteria);
                                                        }
                                                    }}
                            ]
                        });
        avfWindow = isc.Window.create({
                          ID: "avfWindow", title: "Data Record Window", width: 609, height: 625,
                          canDragResize: true, autoCenter: true, isModal: true, showModalMask: true,
                          autoDraw: false,
                          items: [ tsGraphTab ]
                      });

        let tsAddButton = isc.ToolStripButton.create({
                               ID: "tsAddButton", icon: "[SKIN]/actions/add.png", prompt: "Add Row", autoDraw: false, showDown: false,
                               // @ts-ignore
                               click: function() {
                                   const windowAppContext = (window as any)._appContext_;
                                   const windowAppViewGrid = (window as any).appViewGrid;
                                   let appViewGrid: isc.ListGrid;
                                   appViewGrid = windowAppViewGrid;
                                   let lgRecord = appViewGrid.getSelectedRecord();
                                   if (windowAppContext.isGraphEnabled())
                                   {
                                       avfWindow.setTitle("Add New Record Window");
                                       appViewForm.clearValues();
                                       appViewForm.editNewRecord();
                                       const windowTSGraphTab = (window as any).tsGraphTab;
                                       let tsGraphTab: isc.TabSet;
                                       tsGraphTab = windowTSGraphTab;
                                       tsGraphTab.disableTab(1);
                                       RedisGraph.prototype.graphNodeFormsClearValues();
                                   }
                                   else
                                   {
                                       if (lgRecord == null)
                                       {
                                           avfWindow.setTitle("Add New Record Window");
                                           appViewForm.clearValues();
                                           appViewForm.editNewRecord();
                                       }
                                       else
                                       {
                                           let primaryFieldName = "";
                                           let primaryDSField = scDataSource.getPrimaryKeyField();
                                           if (primaryDSField != null)
                                               primaryFieldName = primaryDSField.name;
                                           let keyValueMap = new Map();
                                           for (const [k, v] of Object.entries(lgRecord))
                                           {
                                               if (k == primaryFieldName)
                                                   keyValueMap.set(k, "");
                                               else
                                                   keyValueMap.set(k, v);
                                           }
                                           let defaultValues = {};
                                           keyValueMap.forEach((value, key) => {
                                               // @ts-ignore
                                               defaultValues[key] = value
                                           });
                                           appViewForm.editNewRecord(defaultValues);
                                           avfWindow.setTitle("Add (Duplicate) Record Window");
                                       }
                                   }
                                   appViewForm.clearErrors(false);
                                   avfWindow.show();
                               }
                           });
        let tsEditButton = isc.ToolStripButton.create({
                                 ID: "tsEditButton", icon: "[SKIN]/actions/ai-edit-icon.png", prompt: "Edit Row", autoDraw: false, showDown: false,
                                 // @ts-ignore
                                 click: function() {
                                     const windowAppViewGrid = (window as any).appViewGrid;
                                     let appViewGrid: isc.ListGrid;
                                     appViewGrid = windowAppViewGrid;
                                     if (appViewGrid != null)
                                     {
                                         let lgRecord = appViewGrid.getSelectedRecord();
                                         if (lgRecord != null)
                                         {
                                             appViewForm.clearValues();
                                             appViewForm.clearErrors(false);
                                             appViewForm.editSelectedData(appViewGrid);
                                             avfWindow.setTitle("Edit Data Form Window");
                                             if (windowAppContext.isGraphEnabled())
                                             {
                                                 const windowTSGraphTab = (window as any).tsGraphTab;
                                                 let tsGraphTab: isc.TabSet;
                                                 tsGraphTab = windowTSGraphTab;
                                                 RedisGraph.prototype.appViewToGraphNodeForms();
                                                 tsGraphTab.enableTab(1);
                                                 tsGraphTab.selectTab(0);
                                                 const windowGraphRelForm = (window as any).graphRelForm;
                                                 let graphRelForm: isc.DynamicForm;
                                                 // @ts-ignore
                                                 RedisGraph.prototype.showGraphNodeForm(lgRecord.common_vertex_label);
                                             }
                                             avfWindow.show();
                                         }
                                         else
                                             isc.say("You must select a row on the grid to edit.");
                                     }
                                 }
                             });
        let tsDeleteButton = isc.ToolStripButton.create({
                                  ID: "tsDeleteButton", icon: "[SKIN]/actions/remove.png", prompt: "Delete Row", autoDraw: false, showDown: false,
                                  // @ts-ignore
                                  click: function() {
                                      const windowAppViewGrid = (window as any).appViewGrid;
                                      let appViewGrid: isc.ListGrid;
                                      appViewGrid = windowAppViewGrid;
                                      if (appViewGrid != null)
                                      {
                                          let lgRecord = appViewGrid.getSelectedRecord();
                                          if (lgRecord != null)
                                          {
                                              // @ts-ignore  - You needed to look at the compiled JavaScript to get the callback name correct
                                              isc.confirm("Proceed with row deletion operation?", "RedisGraph.prototype.deleteSelectedAppViewGridRow(value ? 'OK' : 'Cancel')");
                                          }
                                          else
                                              isc.say("You must select a row on the grid to remove.");
                                      }
                                  }
                              });

        // ToolStrip "Details" button and related forms
        let detailViewer = isc.DetailViewer.create({
                                                ID: "detailViewer", width: 400, height: 400, autoDraw: false, dataSource: windowAppContext.getAppViewDS(), showDetailFields: true
                                            });
        let detailLayout = isc.VStack.create({
                                                 ID: "detailLayout", width: "100%", height: "100%", autoDraw: false, layoutAlign: "center",
                                                 layoutTopMargin: 20, layoutBottomMargin: 20, layoutLeftMargin: 20, layoutRightMargin: 20,
                                                 members:[ detailViewer ]
                                             });
        let detailWindow = isc.Window.create({
                                             ID: "detailWindow", title: "Detail Window", width: 465, height: 550, autoCenter: true,
                                             isModal: false, showModalMask: false, canDragResize: true, autoDraw: false,
                                             items: [ detailLayout ]
                                         });
        let tsDetailsButton = isc.ToolStripButton.create({
                                  ID: "tsDetailsButton", icon: "[SKIN]/actions/ai-details-icon.png", prompt: "Row Details", autoDraw: false, showDown: false,
                                 // @ts-ignore
                                 click: function() {
                                     const windowAppViewGrid = (window as any).appViewGrid;
                                     let appViewGrid: isc.ListGrid;
                                     appViewGrid = windowAppViewGrid;
                                     let lgRecord = appViewGrid.getSelectedRecord();
                                     if (lgRecord != null)
                                     {
                                        detailViewer.viewSelectedData(appViewGrid);
                                        detailWindow.show();
                                     }
                                     else
                                         isc.say("You must select a row on the grid to detail.");
                                 }
                              });

        // ToolStrip "Graph Visualization" button and related forms
        let gvHTMLPane = isc.HTMLPane.create({
                                 width:800, height:600, showEdges:false, autoDraw: false,
                                 contentsURL: windowAppContext.getGraphVisualizationURL(),
                                 contentsType:"page"
                             });
        let gvOptionsForm = isc.DynamicForm.create({
                                ID: "gvOptionsForm", autoDraw: false, width: 500, colWidths: [190, "*"],
                                fields: [
                                    {name: "is_matched", title:"Is Matched", type:"text", defaultValue: "false", hidden: true},
                                    {name: "is_hierarchical", title:"Is Hierarchical", type:"text", defaultValue: "false", hidden: true},
                                    {name: "node_shape", title:"Node Shape", type:"text", editorType: "ComboBoxItem", defaultValue: "ellipse", canEdit: true, required: true, hint: "Available node shapes", wrapHintText: false, valueMap : {"ellipse" : "Ellipse", "circle" : "Circle", "box" : "Box", "square" : "Square", "database" : "Database", "text" : "Text", "diamond" : "Diamond", "star" : "Star", "triangle" : "Triangle", "triangleDown" : "Triangle Down", "hexagon" : "Hexagon"}},
                                    // @ts-ignore
                                    {name: "node_color", title:"Node Color", type:"color", defaultValue: "#97C2FC", canEdit: true, required: true, hint: "Available node colors", wrapHintText: false},
                                    {name: "edge_arrow", title:"Edge Arrow", type:"text", editorType: "ComboBoxItem", defaultValue: "to", canEdit: true, required: true, hint: "Available edge arrow types", wrapHintText: false, valueMap : {"to" : "To", "from" : "From", "to;from" : "To and From", "middle" : "Middle"}},
                                    // @ts-ignore
                                    {name: "edge_color", title:"Edge Color", type:"color", defaultValue: "#97C2FC", canEdit: true, required: true, hint: "Available node colors", wrapHintText: false},
                                    // @ts-ignore
                                    {name: "match_color", title:"Match Color", type:"color", defaultValue: "#FAA0A0", canEdit: true, required: true, hint: "Available match colors", wrapHintText: false}
                                ]
                            });
        let gvofApplyButton = isc.IButton.create({
                                  ID: "gvofApplyButton", title: "Apply", autoFit: true, autoDraw: false,
                                  // @ts-ignore
                                  click: function() {
                                      gvofWindow.hide();
                                      gvHTMLPane.setContentsURL(RedisGraph.prototype.graphVisualizationOptionsToURL(false));
                                  }
                              });
        let gvofDefaultsButton = isc.IButton.create({
                                 ID: "gvofDefaultsButton", title: "Defaults", autoFit: true, autoDraw: false,
                                 // @ts-ignore
                                 click: function() {
                                     gvOptionsForm.clearValue("node_shape");
                                     gvOptionsForm.clearValue("node_color");
                                     gvOptionsForm.clearValue("edge_arrow");
                                     gvOptionsForm.clearValue("edge_color");
                                     gvOptionsForm.clearValue("match_color");
                                 }
                             });
        let gvofCancelButton = isc.IButton.create({
                                      ID: "gvofCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                                      // @ts-ignore
                                      click: function() {
                                          gvofWindow.hide();
                                      }
                                  });
        let gvofButtonLayout = isc.HStack.create({
                                     ID: "gvofButtonLayout", width: "100%", height: 24,
                                     layoutAlign: "center", autoDraw: false, membersMargin: 40,
                                     members: [ gvofApplyButton, gvofDefaultsButton, gvofCancelButton ]
                                 });
        let gvoFormLayout = isc.VStack.create({
                                   ID: "gvoFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                   layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                   members:[ gvOptionsForm, gvofButtonLayout ]
                               });
        let gvofWindow = isc.Window.create({
                                   ID: "gvofWindow", title: "Graph Options Window", autoSize: true, autoCenter: true,
                                   isModal: true, showModalMask: true, autoDraw: false,
                                   items: [ gvoFormLayout ]
                               });

        let gvOptionsButton = isc.IButton.create({
                                 ID: "gvOptionsButton", title: "Options", autoFit: true, autoDraw: false,
                                 // @ts-ignore
                                 click: function() {
                                     gvofWindow.show();
                                 }
                             });
        let gvRefreshButton = isc.IButton.create({
                               ID: "gvRefreshButton", title: "Refresh", autoFit: true, autoDraw: false,
                               // @ts-ignore
                               click: function() {
                                   gvHTMLPane.setContentsURL(RedisGraph.prototype.graphVisualizationOptionsToURL(false));
                               }
                           });
        let gvDownloadButton = isc.IButton.create({
                                 ID: "gvDownloadButton", title: "Download", autoFit: true, autoDraw: false,
                                 // @ts-ignore
                                 click: function() {
                                     gvHTMLPane.setContentsURL(RedisGraph.prototype.graphVisualizationOptionsToURL(true));
                                 }
                             });
        let gvCloseButton = isc.IButton.create({
                               ID: "gvCloseButton", title: "Close", autoFit: true, autoDraw: false,
                               // @ts-ignore
                               click: function() {
                                   gvWindow.hide();
                               }
                           });

        let gvButtonLayout = isc.HStack.create({
                               ID: "gvButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                               autoDraw: false, membersMargin: 40, redrawOnResize: true,
                               members: [ gvOptionsButton, gvRefreshButton, gvDownloadButton, gvCloseButton ]
                           });
        let gvFormLayout = isc.VStack.create({
                                 ID: "gvFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                 layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                 members:[ gvHTMLPane, gvButtonLayout ]
                             });

        let gvWindow = isc.Window.create({
                             ID: "gvWindow", title: "Graph Visualization Window", autoSize: true, canDragResize: true,
                             autoCenter: true, isModal: false, showModalMask: false, autoDraw: false, redrawOnResize: true,
                             items: [ gvFormLayout ]
                         });

        let tsGraphButton = isc.ToolStripButton.create({
                                  ID: "tsGraphButton", icon: "[SKIN]/actions/ai-graph-icon.png", prompt: "Show Graph", autoDraw: false, showDown: false,
                                  // @ts-ignore
                                  click: function() {
                                      gvOptionsForm.setValue("is_matched", "false");
                                      gvOptionsForm.setValue("is_hierarchical", "false");
                                      gvWindow.setTitle("Graph Visualization Window");
                                      gvWindow.show();
                                      gvHTMLPane.setContentsURL(RedisGraph.prototype.graphVisualizationOptionsToURL(false));
                                  }
                              });
        let tsGraphMatchedButton = isc.ToolStripButton.create({
                               ID: "tsGraphMatchedButton", icon: "[SKIN]/actions/ai-graph-match-icon.png", prompt: "Show Matched Graph", autoDraw: false, showDown: false,
                               // @ts-ignore
                               click: function() {
                                   gvOptionsForm.setValue("is_matched", "true");
                                   gvOptionsForm.setValue("is_hierarchical", "false");
                                   gvWindow.setTitle("Graph Visualization Window (Matched)");
                                   gvWindow.show();
                                   gvHTMLPane.setContentsURL(RedisGraph.prototype.graphVisualizationOptionsToURL(false));
                               }
                           });
        let tsGraphTreeButton = isc.ToolStripButton.create({
                              ID: "tsGraphTreeButton", icon: "[SKIN]/actions/ai-graph-tree-icon.png", prompt: "Show Graph As Tree", autoDraw: false, showDown: false,
                              // @ts-ignore
                              click: function() {
                                  gvOptionsForm.setValue("is_hierarchical", "true");
                                  gvWindow.setTitle("Graph Visualization Window (Tree)");
                                  gvWindow.show();
                                  gvHTMLPane.setContentsURL(RedisGraph.prototype.graphVisualizationOptionsToURL(false));
                              }
                          });

        // ToolStrip "Application Grid" button
        let tsApplicationGridButton = isc.ToolStripButton.create({
                                         ID: "tsApplicationGridButton", icon: "[SKIN]/actions/ai-application-gird-icon.png", prompt: "Application Grid", autoDraw: false,
                                         // @ts-ignore
                                         click: function() {
                                             const windowAppViewGrid = (window as any).appViewGrid;
                                             const windowCommandGrid = (window as any).commandGrid;
                                             const windowAppLayout = (window as any).appLayout;
                                             let commandGrid: isc.ListGrid;
                                             commandGrid = windowCommandGrid;
                                             let appViewGrid: isc.ListGrid;
                                             appViewGrid = windowAppViewGrid;
                                             let appLayout: isc.VStack;
                                             appLayout = windowAppLayout;
                                             appLayout.showMember(appViewGrid);
                                             appLayout.hideMember(commandGrid);
                                         }
                                     });

        // ToolStrip "Command Grid" button
        let tsCommandGridButton = isc.ToolStripButton.create({
                                     ID: "tsCommandGridButton", icon: "[SKIN]/actions/ai-command-list-icon.png", prompt: "Command Grid", autoDraw: false,
                                     // @ts-ignore
                                     click: function() {
                                         const windowAppViewGrid = (window as any).appViewGrid;
                                         const windowCommandGrid = (window as any).commandGrid;
                                         const windowAppLayout = (window as any).appLayout;
                                         let commandGrid: isc.ListGrid;
                                         commandGrid = windowCommandGrid;
                                         let appViewGrid: isc.ListGrid;
                                         appViewGrid = windowAppViewGrid;
                                         let appLayout: isc.VStack;
                                         appLayout = windowAppLayout;
                                         appLayout.hideMember(appViewGrid);
                                         appLayout.showMember(commandGrid);
                                         let filterMap = new Map();
                                         filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
                                         filterMap.set("_dsStructure", windowAppContext.getDSStructure());
                                         filterMap.set("_appPrefix", windowAppContext.getPrefix());
                                         filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
                                         filterMap.set("_offset", windowAppContext.getCriteriaOffset());
                                         filterMap.set("_limit", windowAppContext.getCriteriaLimit());
                                         filterMap.set("_redisStorageType", windowAppContext.getRedisStorageType());  // row management
                                         filterMap.set("_action", "reload");
                                         let simpleCriteria = {};
                                         filterMap.forEach((value, key) => {
                                             // @ts-ignore
                                             simpleCriteria[key] = value
                                         });
                                         commandGrid.invalidateCache();
                                         commandGrid.filterData(simpleCriteria);
                                     }
                                 });

        // ToolStrip "RedisInsight" button and web page launcher
        let tsRedisInsightButton = isc.ToolStripButton.create({
                                      ID: "tsRedisInsightButton", icon: "[SKIN]/actions/ai-redis-insight-icon.png", prompt: "Redis Insight", autoDraw: false,
                                      // @ts-ignore
                                      click: function() {
                                          window.open(windowAppContext.getRedisInsightURL(), "_blank");
                                      }
                                  });

        // ToolStrip "Settings" button and related forms
        let setGeneralForm = isc.DynamicForm.create({
                                  ID: "setGeneralForm", autoDraw: false, width: 500, colWidths: [190, "*"],
                                  fields: [
                                        {name: "app_group", title:"App Group", type:"text", value: windowAppContext.getGroupName(), canEdit: true, required: true, hint: "Application group", wrapHintText: false},
                                        {name: "app_name", title:"App Name", type:"text", value: windowAppContext.getAppName(), canEdit: true, required: true, hint: "Application name", wrapHintText: false},
                                        {name: "ds_name", title:"DS Name", type:"text", value: windowAppContext.getAppViewDS(), canEdit: false, hint: "Data source name", wrapHintText: false},
                                        {name: "ds_title", title:"DS Title", type:"text",  value: windowAppContext.getAppViewTitle(), canEdit: false, required: true, hint: "Data source title", wrapHintText: false}
                                      ]
                            });
        let setGridForm = isc.DynamicForm.create({
                                    ID: "setGridForm", autoDraw: false, width: 500, colWidths: [190, "*"], isGroup: true, groupTitle: "Grid Options",
                                    fields: [
                                        // @ts-ignore
                                        {name: "fetch_policy", title: "Fetch Policy", type: "SelectItem", hint: "Page navigation", canEdit: false, wrapHintText: false, defaultValue: "Virtual Paging", valueMap : {"virtual" : "Virtual Paging", "paging" : "Single Paging"}},
                                        // @ts-ignore
                                        {name: "page_size", title: "Page Size", editorType: "SpinnerItem", writeStackedIcons: true, canEdit: false, hint: "Rows per page", wrapHintText: false, defaultValue: 50, min: 19, max: 100, step: 10},
                                        // @ts-ignore
                                        {name: "column_filtering", title: "Column Filtering", type: "radioGroup", defaultValue: "Disabled", valueMap: ["Disabled", "Enabled"], vertical:false},
                                        // @ts-ignore
                                        {name: "csv_header", title: "CSV Header", type: "radioGroup", defaultValue: "Title", valueMap: ["Title", "Field/Type/Title"], vertical:false}
                                    ]
                                });
        let settingsSaveButton = isc.Button.create({
                                         ID: "settingsSaveButton", title: "Save", autoFit: true, autoDraw: false,
                                         // @ts-ignore
                                         click: function() {
                                             if (setGeneralForm.valuesAreValid(false, false))
                                             {
                                                 const windowAppContext = (window as any)._appContext_;
                                                 windowAppContext.setGroupName(setGeneralForm.getValue("app_group"));
                                                 windowAppContext.setAppName(setGeneralForm.getValue("app_name"));
                                                 const windowHeaderHTMLFlow = (window as any).headerHTMLFlow;
                                                 let headerHTMLFlow: isc.HTMLFlow;
                                                 headerHTMLFlow = windowHeaderHTMLFlow;
                                                 headerHTMLFlow.setContents(RedisGraph.prototype.createHTMLHeader());
                                                 headerHTMLFlow.redraw();
                                                 const windowAppViewGrid = (window as any).appViewGrid;
                                                 let appViewGrid: isc.ListGrid;
                                                 appViewGrid = windowAppViewGrid;
                                                 if (setGridForm.getValue("column_filtering") == "Enabled")
                                                     appViewGrid.setShowFilterEditor(true);
                                                 else
                                                     appViewGrid.setShowFilterEditor(false);
                                                 if (setGridForm.getValue("csv_header") == "Title")
                                                     windowAppContext.setGridCSVHeader("title");
                                                 else
                                                     windowAppContext.setGridCSVHeader("field");
                                                 // @ts-ignore
                                                 settingsWindow.hide();
                                             }
                                             else
                                                 setGeneralForm.validate(false);
                                         }
                                     });
        let settingsCancelButton = isc.IButton.create({
                                            ID: "settingsCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                                            // @ts-ignore
                                            click: function() {
                                                settingsWindow.hide();
                                            }
                                        });
        let settingsButtonLayout = isc.HStack.create({
                                           ID: "settingsButtonLayout", width: "100%", height: 24,
                                           layoutAlign: "center", autoDraw: false, membersMargin: 40,
                                                 members: [ settingsSaveButton, settingsCancelButton ]
                                       });
        let settingsFormLayout = isc.VStack.create({
                                             ID: "settingsFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                                   layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                             members:[ setGeneralForm, setGridForm, settingsButtonLayout ]
                                         });
        let settingsWindow = isc.Window.create({
                                         ID: "settingsWindow", title: "Settings Window", autoSize: true, autoCenter: true,
                                         isModal: true, showModalMask: true, autoDraw: false,
                                         items: [ settingsFormLayout ]
                                     });
        let tsSettingsButton = isc.ToolStripButton.create({
                                  ID: "tsSettingsButton", icon: "[SKIN]/actions/ai-settings-gear-black-icon.png", prompt: "Settings", autoDraw: false,
                                  // @ts-ignore
                                  click: function() {
                                      setGeneralForm.clearErrors(false);
                                      settingsWindow.show();
                                  }
                              });

        // ToolStrip "Help" button and web page launcher
        let tsHelpButton = isc.ToolStripButton.create({
                                 ID: "tsHelpButton", icon: "[SKIN]/actions/ai-help-icon.png", prompt: "Online Help", autoDraw: false,
                                 // @ts-ignore
                                 click: function() {
                                     let offset = window.location.href.lastIndexOf("/") + 1;
                                     let urlBase = window.location.href.substring(0, offset);
                                     let urlHelpDoc = urlBase + "doc/UG-RedisAppStudio.pdf";
                                     window.open(urlHelpDoc, "_blank");
                                 }
                             });

        // ToolStrip creation and button assignment
        let commandToolStrip : isc.ToolStrip;
        commandToolStrip = isc.ToolStrip.create({
                                // @ts-ignore
                                ID: "commandToolStrip", width: "100%", height: "3%", autoDraw: false,
                                // @ts-ignore
                                members: [fileMenuButton, "separator", tsSchemaButton, "separator", tsAddButton, tsEditButton, tsDeleteButton, tsDetailsButton, "separator", tsGraphButton, tsGraphMatchedButton, tsGraphTreeButton, "starSpacer", tsApplicationGridButton, tsCommandGridButton, tsRedisInsightButton, tsSettingsButton, tsHelpButton]
                            });
        return commandToolStrip;
    }

    // Create the main data grid
    private createAppViewGrid(): isc.ListGrid
    {
        const windowAppContext = (window as any)._appContext_;

        // @ts-ignore
        let CustomListGrid = isc.defineClass("CustomListGrid", "ListGrid").addProperties({
                init: function () {
                            this.Super("init", arguments);
                            var toolStrip = isc.ToolStrip.create({
                                 membersMargin: 5, autoDraw: false,
                                 members: [
                                     isc.Label.create({
                                              ID: "gridPosition",
                                              wrap: false, padding: 5, autoDraw: false,
                                              contents: "0 to 0 of 0",
                                              // @ts-ignore
                                              getRowRangeText: function(arrayVisibleRows, totalRows, lengthIsKnown) {
                                                  if (! lengthIsKnown)
                                                      return "Loading...";
                                                  else if (arrayVisibleRows[0] != -1)
                                                  {
                                                      let adjTotalRows = totalRows - 1;
                                                      return isc.NumberUtil.format((arrayVisibleRows[0] + 1), "#,##0") + " to " + isc.NumberUtil.format((arrayVisibleRows[1]), "#,##0") + " of " + isc.NumberUtil.format(adjTotalRows, "#,##0");
                                                  }
                                                  else
                                                      return "0 to 0 of 0";
                                              }
                                          }),
                                     isc.LayoutSpacer.create({width: "*"}),
                                     // @ts-ignore
                                     "separator",
                                     isc.ImgButton.create({
                                                  grid: this, src: "[SKIN]/actions/refresh.png", showRollOver: false,
                                                  prompt: "Refresh", width: 16, height: 16, showDown: false, autoDraw: false,
                                                  // @ts-ignore
                                                  click: function () {
                                                      RedisGraph.prototype.executeAppViewGraphSearch();
                                                  }
                                              })
                                 ]

                             });
                            this.setProperty("gridComponents", ["filterEditor", "header", "body", "summaryRow", toolStrip]);

                        },
                        initWidget: function () {
                            this.Super("initWidget", arguments);
                            this.observe(this, "dataChanged", function () {
                                this.updateRowRangeDisplay();
                            });
                            this.observe(this, "scrolled", function () {
                                this.updateRowRangeDisplay();
                            });
                        },
                        updateRowRangeDisplay: function () {
                            var label = this.gridComponents[4].getMember(0);
                            label.setContents(label.getRowRangeText(this.getVisibleRows(), this.getTotalRows(), this.data.lengthIsKnown()));
                        }
                    });

        let appViewGrid : isc.ListGrid;
        // @ts-ignore
        appViewGrid = CustomListGrid.create({
                        ID: "appViewGrid", dataSource: windowAppContext.getAppViewDS(),
                        autoDraw: false, width: "100%", height: windowAppContext.getGridHeightPercentage(),
                        autoFetchData: windowAppContext.isModelerEnabled(), showFilterEditor: false,
                        allowFilterOperators: false, filterOnKeypress: true, useAdvancedFieldPicker: true,
                        canEditTitles: true, expansionFieldImageShowSelected: false, canExpandRecords: true,
                        expansionMode: "related", detailDS: windowAppContext.getAppViewRelDS(), canEdit: false, leaveScrollbarGap: false,
                        initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                        recordDoubleClick: function () {
                            const windowAppViewGrid = (window as any).appViewGrid;
                            const windowDetailViewer = (window as any).detailViewer;
                            const windowDetailWindow = (window as any).detailWindow;
                            let appViewGrid: isc.ListGrid;
                            appViewGrid = windowAppViewGrid;
                            let detailViewer: isc.DetailViewer;
                            detailViewer = windowDetailViewer;
                            let detailWindow: isc.Window;
                            detailWindow = windowDetailWindow;
                            let lgRecord = appViewGrid.getSelectedRecord();
                            if (lgRecord != null)
                            {
                                detailViewer.viewSelectedData(appViewGrid);
                                detailWindow.show();
                            }
                            else
                                isc.say("You must select a row on the grid to detail.");
                        }
                    });

        return appViewGrid;
    }

    // Create the Application View Grid
    private createCommandGrid(): isc.ListGrid
    {
        const windowAppContext = (window as any)._appContext_;

        let commandGrid = isc.ListGrid.create({
                                  ID: "commandGrid", dataSource: "RG-DocCmdGrid", autoDraw: false, width: "100%",
                                  height: windowAppContext.getGridHeightPercentage(), autoFetchData: false,
                                  showFilterEditor: false, allowFilterOperators: false, filterOnKeypress: false,
                                  useAdvancedFieldPicker: false, canEditTitles: false, expansionFieldImageShowSelected: false,
                                  canExpandRecords: false, canEdit: false, leaveScrollbarGap: false, wrapCells: true, cellHeight: 50,
                                  alternateRecordStyles:true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
                                  // @ts-ignore
                                  recordDoubleClick: function () {
                                      const windowCommandGrid = (window as any).commandGrid;
                                      let commandGrid: isc.ListGrid;
                                      commandGrid = windowCommandGrid;
                                      let lgRecord = commandGrid.getSelectedRecord();
                                      if (lgRecord != null)
                                      {
                                          // @ts-ignore
                                          window.open(lgRecord.command_link, "_blank");
                                      }
                                      else
                                          isc.say("You must select a row on the grid to show help details.");
                                  },
                                  // @ts-ignore
                                  getCellCSSText: function (record, rowNum, colNum) {
                                      if (this.getFieldName(colNum) == "redis_command")
                                          return "font-weight:bold; color:#000000;";
                                      else if (this.getFieldName(colNum) == "redis_parameters")
                                          return "font-weight:lighter; font-style: italic; color:#000000;";
                                  }
                              });

        return commandGrid;
    }

    // Initializes all UI controls for the application
    init(): void
    {
        // The following allows you to assign default font and UI control sizes
        // Invoke for Dense density: isc.Canvas.resizeFonts(1); isc.Canvas.resizeControls(2);
        // Invoke for Compact density: isc.Canvas.resizeFonts(2); isc.Canvas.resizeControls(4);
        // Invoke for Medium density: isc.Canvas.resizeFonts(2); isc.Canvas.resizeControls(6);
        // Invoke for Spacious density: isc.Canvas.resizeFonts(3); isc.Canvas.resizeControls(10);
        isc.Canvas.resizeFonts(1);
        isc.Canvas.resizeControls(2);
        isc.Notify.configureMessages("message", {multiMessageMode: "replace", autoFitMaxWidth: 250, slideSpeed: 200});

        // Create our UI components
        // Do not enable this without adding a timer to prevent concurrent fetches while your SessionContext is being created.
        // let appPropGrid = isc.ListGrid.create({ID: "appPropGrid", dataSource: "RW-AppPropGrid", autoFetchData: true, visibility: "hidden"});
        const windowAppContext = (window as any)._appContext_;

        let headerSection = this.createHeaderSection();
        let searchSection = this.createSearchSection();
        let commandToolStrip = this.createCommandToolStrip();
        let appViewGrid = this.createAppViewGrid();
        let commandGrid = this.createCommandGrid();

        this.appLayout = isc.VStack.create({
                                       ID: "appLayout", width: "100%", height: "100%", autoDraw: false, layoutTopMargin: 5, membersMargin: 0,
                                       members: [headerSection, searchSection, commandToolStrip, appViewGrid, commandGrid]
                                   });
        this.appLayout.hideMember(commandGrid);
    }

    // Shows the application
    show(): void
    {
        this.appLayout.show();
    }

    // Hides the application
    hide(): void
    {
        this.appLayout.hide();
    }
}