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
 * set prior to storing it in Redis. The RedisAppStudio class defines and
 * manages the complete SmartClient application. While this logic could
 * be broken out to other smaller files, the decision was made to keep it
 * self-contained to simplify application generation and minimize browser
 * load times.
 */
class RedisAppStudio
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

        return "<table class=\"ahTable\"> <col width=\"1%\"> <col width=\"5%\"> <col width=\"15%\">" +
            " <col width=\"58%\"> <col width=\"20%\"> <col width=\"1%\"> <tr> <td>&nbsp;</td>" +
            "  <td><img alt=\"Redis Logo\" class=\"ahImage\" src=\"images/redis-small.png\" width=\"99\" height=\"60\"></td>" +
            "  <td class=\"ahGroup\">" + windowAppContext.getGroupName() + "</td> <td>&nbsp;</td>" +
            "  <td class=\"ahName\">" + windowAppContext.getAppName() + "</td>\n" +
            "  <td>&nbsp;</td> </tr> </table>";
    }

    // Generates the application header section of the UI
    private createHeaderSection(): isc.HTMLFlow
    {
        let htmlString = this.createHTMLHeader();
        let headerHTMLFlow = isc.HTMLFlow.create({ID:"headerHTMLFlow", width:"100%", height: "5%", autoDraw: false, contents:htmlString});

        return headerHTMLFlow;
    }

    // Creates multiple methods for search the AppView grid data
    private createSearchSection(): isc.SectionStack
    {
        // Criteria search section
        let searchForm = isc.DynamicForm.create({
                                        ID:"searchForm", autoDraw: false, iconWidth: 16, iconHeight: 16,
                                        items: [{
                                            type: "text", name: "search_terms", title: "Search Term(s)", wrapTitle: false, width: 300, suppressBrowserClearIcon:true,
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
        let searchFilter = isc.FilterBuilder.create({
                                          ID:"searchFilter", width: 500, height: 150, autoDraw: false,
                                          topOperatorAppearance: "none", showSubClauseButton: false
                                      });
        let fbSearchButton = isc.Button.create({
                                                  ID: "fbSearchButton", title: "Search", autoFit: true, autoDraw: false});
        let fbApplyButton = isc.Button.create({
                                                 ID: "fbApplyButton", title: "Apply", autoFit: true, autoDraw: false});
        let fbResetButton = isc.Button.create({
                                                  ID: "fbResetButton", title: "Reset", autoFit: true, autoDraw: false});
        let fbCancelButton = isc.IButton.create({
                                                    ID: "fbCancelButton", title: "Cancel", autoFit: true, autoDraw: false});
        let fbButtonLayout = isc.HStack.create({
                                                   ID: "fbButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                                                   autoDraw: false, membersMargin: 40,
                                                   members: [ fbSearchButton, fbApplyButton, fbResetButton, fbCancelButton ]
                                               });

        let fbFormLayout = isc.VStack.create({
                                                 ID: "fbFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                                 layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                                 members:[ searchFilter, fbButtonLayout ]
                                             });

        let fbWindow = isc.Window.create({
                                             ID: "fbWindow", title: "Filter Builder Window", autoSize: true, autoCenter: true,
                                             isModal: false, showModalMask: false, autoDraw: false,
                                             items: [ fbFormLayout ]
                                         });
        let tsAdvancedSearch = isc.ToolStripButton.create({
                                         ID: "tsSearchAdvanced", icon: "[SKIN]/actions/ai-search-advanaced-icon.png", prompt: "Advanced Criteria Form", showDown: false, autoDraw: false});
        let tsHighlightSearch = isc.ToolStripButton.create({
                                         ID: "tsHighlightSearch", icon: "[SKIN]/actions/ai-highlight-off-icon.png", prompt: "Highlight matches", showDown: false, actionType: "checkbox", showFocusOutline: false, autoDraw: false,
                                         // @ts-ignore
                                         click: function()
                                         {
                                             if (tsHighlightSearch.isSelected())
                                                 tsHighlightSearch.setBackgroundColor("white");
                                         }
                                     });
        let tsPhoneticSearch = isc.ToolStripButton.create({
                                       ID: "tsPhoneticSearch", icon: "[SKIN]/actions/ai-phonetic-off-icon_Selected.png", prompt: "Phonetic search", showDown: false, actionType: "checkbox", showFocusOutline: false, autoDraw: false,
                                       // @ts-ignore
                                       click: function()
                                       {
                                           if (tsPhoneticSearch.isSelected())
                                               tsPhoneticSearch.setBackgroundColor("white");
                                       }
                                   });
        let tsExecuteSearch = isc.ToolStripButton.create({
                                          ID: "tsSearchExecute", icon: "[SKIN]/pickers/search_picker.png", prompt: "Execute Search", showDown: false, autoDraw: false});
        let tsClearSearch = isc.ToolStripButton.create({
                                       ID: "tsSearchClear", icon: "[SKIN]/pickers/clear_picker.png", prompt: "Clear Search Criteria", showDown: false, autoDraw: false,
                                       // @ts-ignore
                                       click: function()
                                       {
                                           tsHighlightSearch.deselect();
                                           tsPhoneticSearch.deselect();
                                       }
                                     });
        let tsSearch = isc.ToolStrip.create({
                                        ID: "tsSearch", border: "0px", backgroundColor: "white", autoDraw: false,
                                         // @ts-ignore
                                         members: [tsAdvancedSearch, tsHighlightSearch, tsPhoneticSearch, tsExecuteSearch, tsClearSearch]
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
                                                    editorType: "ComboBoxItem"
                                                    }]
                                                });
        let tsClearSuggestion = isc.ToolStripButton.create({
                                       ID: "tsClearSuggestion", icon: "[SKIN]/pickers/clear_picker.png", prompt: "Clear Search Suggestion", showDown: false, autoDraw: false,
                                       // @ts-ignore
                                       click: function()
                                       {
                                           tsHighlightSearch.deselect();
                                           tsPhoneticSearch.deselect();
                                       }
                                   });
        let tsSuggest = isc.ToolStrip.create({
                                                 ID: "tsSuggest", border: "0px", backgroundColor: "white", autoDraw: false,
                                                // @ts-ignore
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

    // Create the ToolStrip component with command buttons
    private createCommandToolStrip(): isc.ToolStrip
    {
        const windowAppContext = (window as any)._appContext_;

        // ToolStrip "File" Menu Item - Document upload form and grid
        let docUploadForm = isc.DynamicForm.create({
                                                      ID: "docUploadForm", width: 275, height: 75, autoDraw: false,
                                                       fields: [
                                                           {name: "document_title", title:"Title", type:"text", required: true},
                                                           {name: "document_description", title:"Description", type:"text", defaultValue: "None", required: true},
                                                           {name: "document_file", title:"File", type:"binary", required: true}
                                                       ]
                                                  });
        let dufSaveButton = isc.Button.create({
                                                  ID: "dufSaveButton", title: "Upload", autoFit: true, autoDraw: false,
                                                  // @ts-ignore
                                                  click: function() {
                                                      if (docUploadForm.valuesAreValid(false, false))
                                                          dufWindow.hide();
                                                      else
                                                          docUploadForm.validate(false);
                                                  }
                                              });
        let dufCancelButton = isc.IButton.create({
                                                     ID: "dufCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                                                     // @ts-ignore
                                                     click: function() {
                                                         dufWindow.hide();
                                                     }
                                                 });
        let dufButtonLayout = isc.HStack.create({
                                                    ID: "dufButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                                                    autoDraw: false, membersMargin: 40,
                                                    members: [ dufSaveButton, dufCancelButton ]
                                                });
        let docUploadFormLayout = isc.VStack.create({
                                                       ID: "docUploadFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                                       layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                                       members:[ docUploadForm, dufButtonLayout ]
                                                   });
        let dufWindow = isc.Window.create({
                                              ID: "dufWindow", title: "Document Upload Form Window", autoSize: true,
                                              autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
                                              items: [ docUploadFormLayout ]
                                          });
        let documentsGrid = isc.ListGrid.create({
                                                 ID:"documentsGrid", width: 700, height: 300, autoDraw: false, dataSource: "RS-DocumentGrid",
                                                 autoFetchData: true, canRemoveRecords: false, canSort: false, autoFitData: "horizontal",
                                                 alternateRecordStyles:true, showHeaderContextMenu: false, autoSaveEdits: true, canEdit: true,
                                                 wrapCells: true, cellHeight: 36, editEvent: "doubleClick", listEndEditAction: "done",
                                                 fields:[
                                                     {name:"document_name", title:"Name"},
                                                     {name:"document_title", title:"Title"},
                                                     {name:"document_type", title:"Type"},
                                                     {name:"document_description", title:"Description"},
                                                     {name:"document_date", title:"Upload Date"},
                                                     {name:"document_size", title:"File Size"}
                                                 ]
                                             });
        let dgAddButton = isc.Button.create({
                                                  ID: "dgAddButton", title: "Add", autoFit: true, autoDraw: false, disabled: true});
        let dgDeleteButton = isc.Button.create({
                                                ID: "dgDeleteButton", title: "Delete", autoFit: true, autoDraw: false});
        let dgCloseButton = isc.IButton.create({
                                                   ID: "dgCloseButton", title: "Close", autoFit: true, autoDraw: false,
                                                   // @ts-ignore
                                                   click: function() {
                                                       dgWindow.hide();
                                                   }
                                               });
        let dgButtonLayout = isc.HStack.create({
                                                   ID: "dgButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                                                   autoDraw: false, membersMargin: 40,
                                                   members: [ dgAddButton, dgDeleteButton, dgCloseButton ]
                                               });
        let dgFormLayout = isc.VStack.create({
                                                 ID: "dgFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                                 layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                                 members:[ documentsGrid, dgButtonLayout ]
                                             });
        let dgWindow = isc.Window.create({
                                             ID: "dgWindow", title: "Documents Manager Window", autoSize: true,
                                             autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
                                             items: [ dgFormLayout ]
                                         });
        // ToolStrip "File" menu
        let fileMenu = isc.Menu.create({
                         ID: "fileMenu", showShadow: true, shadowDepth: 10, autoDraw: false,
                         data: [
                             {title: "Document ...", icon: "[SKIN]/actions/ai-document-commands-icon.png",
                                 submenu: [
                                     {title: "Upload", enabled: true, icon: "[SKIN]/actions/ai-document-upload-icon.png"},
                                     {title: "Manage", enabled: true, icon: "[SKIN]/actions/ai-document-manage-icon.png", click: function() {
                                         documentsGrid.deselectAllRecords();
                                         dgWindow.show();
                                     }}
                                 ]},
                             {isSeparator: true},
                             {title: "Redis Data ...", icon: "[SKIN]/actions/ai-save-icon.png",
                                 submenu: [
                                     {title: "Connect", icon: "[SKIN]/actions/ai-redis-connect-icon.png", enabled: true, checked: true},
                                     {title: "Delete", icon: "[SKIN]/actions/ai-commands-delete-icon.png", enabled: false},
                                     {title: "Disconnect", icon: "[SKIN]/actions/ai-redis-disconnect-icon.png", enabled: false, checked: false}
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
                                     {title: "Grid as CSV", icon: "[SKIN]/actions/ai-export-grid-csv-icon.png"},
                                     {title: "Grid as JSON", icon: "[SKIN]/actions/ai-export-grid-json-icon.png"},
                                     {title: "Schema as XML", icon: "[SKIN]/actions/ai-export-schema-xml-icon.png"},
                                     {title: "Commands as TXT", icon: "[SKIN]/actions/ai-commands-icon.png"}
                                ]}
                         ]
                     });
        let fileMenuButton = isc.ToolStripMenuButton.create({
                              ID: "fileMenuButton", title: "File", autoDraw: false, menu: fileMenu
                          });

        // ToolStrip "Schema" button and related grid and forms
        let schemaGrid = isc.ListGrid.create({
                             ID:"schemaGrid", width: 710, height: 500, autoDraw: false, canEdit: true, canSort: false, alternateRecordStyles:true,
                             showHeaderContextMenu: false, editEvent: "click", listEndEditAction: "next", autoSaveEdits: false});
        let sgApplyButton = isc.Button.create({
                                                  ID: "sgApplyButton", title: "Update", autoFit: true, autoDraw: false});
        let sgDiscardButton = isc.Button.create({
                                                  ID: "sgDiscardButton", title: "Discard", autoFit: true, autoDraw: false});
        let sgRebuildButton = isc.Button.create({
                                                    ID: "sgRebuildButton", title: "Rebuild", autoFit: true, autoDraw: false});
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
                                                   members: [ sgApplyButton, sgRebuildButton, sgDiscardButton, sgCloseButton ]
                                               });
        let sgFormLayout = isc.VStack.create({
                                                 ID: "sgFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                                 layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                                 members:[ schemaGrid, sgButtonLayout ]
                                             });
        let sgWindow = isc.Window.create({
                                             ID: "sgWindow", title: "Schema Editor Window", autoSize: true, autoCenter: true,
                                             isModal: true, showModalMask: true, autoDraw: false,
                                             items: [ sgFormLayout ]
                                         });
        let tsSchemaButton = isc.ToolStripButton.create({
                                 ID: "tsSchemaButton", icon: "[SKIN]/actions/ai-schema-icon.png", prompt: "Schema Form", showDown: false, autoDraw: false,
                                 // @ts-ignore
                                click: function()
                                {
                                    sgWindow.show();
                                }
                             });

        // ToolStrip "Add, Edit and Delete" buttons and related forms
        let appViewForm = isc.DynamicForm.create({
                                 ID: "appViewForm", width: 300, height: 300, numCols:2, autoDraw: false});
        let avfSaveButton = isc.Button.create({
                              ID: "avfSaveButton", title: "Save", autoFit: true, autoDraw: false,
                              // @ts-ignore
                              click: function() {
                                  if (appViewForm.valuesAreValid(false, false))
                                      avfWindow.hide();
                                  else
                                      appViewForm.validate(false);
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
                               ID: "avfButtonLayout", width: "100%", height: 24, layoutAlign: "center", autoDraw: false, membersMargin: 40,
                               members: [ avfSaveButton, avfCancelButton ]
                          });
        let appViewFormLayout = isc.VStack.create({
                                ID: "appViewFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                members:[ appViewForm, avfButtonLayout ]
                           });
        let avfWindow = isc.Window.create({
                                 ID: "avfWindow", title: "Search Form Window", autoSize: true, canDragResize: true,
                                 autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
                                 items: [ appViewFormLayout ]
                             });
        let tsAddButton = isc.ToolStripButton.create({
                               ID: "tsAddButton", icon: "[SKIN]/actions/add.png", prompt: "Add Row", autoDraw: false, showDown: false});
        let tsEditButton = isc.ToolStripButton.create({
                                 ID: "tsEditButton", icon: "[SKIN]/actions/ai-edit-icon.png", prompt: "Edit Row", autoDraw: false, showDown: false});
        let tsDeleteButton = isc.ToolStripButton.create({
                                  ID: "tsDeleteButton", icon: "[SKIN]/actions/remove.png", prompt: "Delete Row", autoDraw: false, showDown: false});

        // ToDo: ToolStrip "Document View" button
        let tsViewButton = isc.ToolStripButton.create({
                                  ID: "tsViewButton", icon: "[SKIN]/actions/ai-commands-view-icon.png", prompt: "View Document", autoDraw: false, showDown: false,
                                  // @ts-ignore
                                  click: function() {isc.say('Document viewing is not enabled for this configuration.');}
                              });

        // ToolStrip "Details" button and related forms
        let detailViewer = isc.DetailViewer.create({
                                                ID: "detailViewer", width: 400, height: 400, autoDraw: false, showDetailFields: true
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
                                  ID: "tsDetailsButton", icon: "[SKIN]/actions/ai-details-icon.png", prompt: "Row Details", autoDraw: false, showDown: false});

        // ToDo: ToolStrip "Map" button
        let tsMapButton = isc.ToolStripButton.create({
                                   ID: "tsMapButton", icon: "[SKIN]/actions/ai-maps-icon.png", prompt: "Show Map", autoDraw: false,
                                   // @ts-ignore
                                   click: function() {isc.say('Map viewing is not enabled for this configuration.');}
                               });

        let tsGridView = isc.DynamicForm.create({
                                ID: "tsGridView", showResizeBar:false, width:245, minWidth:245, autoDraw: false,
                                fields: [
                                    {name: "gridView", title: "Grid View", showTitle: true, width:"*",
                                        valueMap: {
                                            "application": "Application",
                                                  // "chart": "Data Chart", - defer for TimeSeries
                                               "commands": "Redis Commands"
                                        },
                                        defaultValue:"Application",
                                        disabled: true}
                                ]
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
                                        {name: "ds_name", title:"DS Name", type:"text", value: windowAppContext.getDSAppViewName(), canEdit: false, hint: "Data source name", wrapHintText: false},
                                        {name: "ds_title", title:"DS Title", type:"text",  value: windowAppContext.getDSAppViewTitle(), canEdit: false, required: true, hint: "Data source title", wrapHintText: false}
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
                                        {name: "csv_header", title: "CSV Header", type: "radioGroup", defaultValue: "Title", valueMap: ["Title", "Field/Type/Title"], vertical:false},
                                        // @ts-ignore
                                        {name: "highlight_color", title:"Highlight Color", type:"color", defaultValue: windowAppContext.getHighlightFontColor() },
                                        // @ts-ignore
                                        {name: "facet_count", title: "Facet Count", editorType: "SpinnerItem", writeStackedIcons: false, defaultValue: 10, min: 3, max: 20, step: 1},
                                    ]
                                });
        let settingsSaveButton = isc.Button.create({
                                                 ID: "settingsSaveButton", title: "Save", autoFit: true, autoDraw: false,
                                                 // @ts-ignore
                                                 click: function() {
                                                     if (setGeneralForm.valuesAreValid(false, false))
                                                         // @ts-ignore
                                                         settingsWindow.hide();
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
                                  ID: "tsSettingsButton", icon: "[SKIN]/actions/ai-settings-gear-black-icon.png", prompt: "Settings", autoDraw: false});

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
        let commandToolStrip = isc.ToolStrip.create({
                             // @ts-ignore
                             ID: "commandToolStrip", width: "100%", height: "3%", autoDraw: false,
                             // @ts-ignore
                             members: [fileMenuButton, "separator", tsSchemaButton, "separator", tsAddButton, tsEditButton, tsDeleteButton, tsDetailsButton, tsViewButton, tsMapButton, "separator", "starSpacer", tsGridView, tsRedisInsightButton, tsSettingsButton, tsHelpButton]
                         });

        return commandToolStrip;
    }

    // Create the Application View Grid Layout
    private createAppViewGridLayout(): isc.HLayout
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
                                                                                               // return (arrayVisibleRows[0]+1) + " to "+ (arrayVisibleRows[1]+1) + " of " + isc.NumberUtil.format(totalRows, "#,##0");
                                                                                               return isc.NumberUtil.format((arrayVisibleRows[0]+1), "#,##0") + " to "+ isc.NumberUtil.format((arrayVisibleRows[1]+1), "#,##0") + " of " + isc.NumberUtil.format(totalRows, "#,##0");
                                                                                           else
                                                                                               return "0 to 0 of 0";
                                                                                       }
                                                                                   }),
                                                                  isc.LayoutSpacer.create({width: "*"}),
                                                                  // @ts-ignore
                                                                  "separator",
                                                                  isc.ImgButton.create({
                                                                                           // @ts-ignore
                                                                                           grid: this, src: "[SKIN]/actions/refresh.png", showRollOver: false,
                                                                                           prompt: "Refresh", width: 16, height: 16, showDown: false, autoDraw: false})
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

        let appFacetGrid = isc.TreeGrid.create({
                               // @ts-ignore
                               ID: "appFacetGrid",
                               autoDraw: false, width: "15%", autoFetchData: false, showConnectors: true,
                               showResizeBar: true, useAdvancedCriteria: false,
                               // @ts-ignore
                               data: isc.Tree.create({
                                      modelType: "parent", nameProperty: "facet_name",
                                      idField: "id", parentIdField: "parent_id",
                                      /* @ts-ignore */
                                      data: [ {id:"1", parent_id:"0", facet_name:"Facet List"}]
                                  }),
                               fields: [
                                   {name: "facet_name", title: "Filter By Facets"}
                               ]});

        // @ts-ignore
        let appViewGrid = CustomListGrid.create({
                            ID: "appViewGrid", autoDraw: false, showFilterEditor: false, allowFilterOperators: false,
                            filterOnKeypress: true, useAdvancedFieldPicker: true, canEditTitles: true,
                            expansionFieldImageShowSelected: false, canExpandRecords: false, canEdit: false,
                            leaveScrollbarGap: false,
                            fields:[
                                {name:"full_name", title:"Full Name"},
                                {name:"position_title", title:"Position Title"},
                                {name:"office_location", title:"Office Location"},
                                {name:"industry_focus", title:"Industry Focus"},
                                {name:"salary", title:"Salary"},
                                {name:"place_name", title:"Place Name"},
                                {name:"city", title:"City"},
                                {name:"state", title:"State"},
                                {name:"region", title:"Region"}
                            ]});

        let appViewGridLayout: isc.HStack;
        if (windowAppContext.isFacetUIEnabled())
        {
            appViewGrid.setWidth("85%");
            appViewGridLayout = isc.HStack.create({
                                      ID: "appViewGridLayout", width: "100%", height: windowAppContext.getGridHeightPercentage(), autoDraw: false,
                                      members: [ appFacetGrid, appViewGrid ]
                                  });
        }
        else
        {
            appViewGrid.setWidth("100%");
            appViewGridLayout = isc.HStack.create({
                                      ID: "appViewGridLayout", width: "100%", height: windowAppContext.getGridHeightPercentage(), autoDraw: false,
                                      members: [ appViewGrid ]
                                  });
        }

        return appViewGridLayout;
    }

// Create the Redis Command Grid
    private createCommandGrid(): isc.ListGrid
    {
        const windowAppContext = (window as any)._appContext_;

        let commandGrid = isc.ListGrid.create({
                            ID: "commandGrid", autoDraw: false, width: "100%", height: windowAppContext.getGridHeightPercentage(),
                            autoFetchData: false, showFilterEditor: false, allowFilterOperators: false, filterOnKeypress: false,
                            useAdvancedFieldPicker: false, canEditTitles: false, expansionFieldImageShowSelected: false,
                            canExpandRecords: false, canEdit: false, leaveScrollbarGap: false, wrapCells: true, cellHeight: 50,
                            fields:[
                              {name:"id", title:"Timestamp"},
                              {name:"redis_command", title:"Command Name"},
                              {name:"redis_parameters", title:"Command Parameters"},
                              {name:"command_link", title:"Documentation Link"},
                              {name:"command_description", title:"Description"}
                            ]});

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
        isc.Canvas.resizeFonts(1); isc.Canvas.resizeControls(2);
        isc.Notify.configureMessages("message", {multiMessageMode: "replace", autoFitMaxWidth: 250, slideSpeed: 200});

        // Create our UI components
        let headerSection = this.createHeaderSection();
        let searchSection = this.createSearchSection();
        let commandToolStrip = this.createCommandToolStrip();
        let appViewGridLayout = this.createAppViewGridLayout();
        let commandGrid = this.createCommandGrid();
        this.appLayout = isc.VStack.create({
                                    ID: "appLayout", width: "100%", height: "100%", autoDraw: false, layoutTopMargin: 5, membersMargin: 0,
                                    members:[ headerSection, searchSection, commandToolStrip, appViewGridLayout, commandGrid ]
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