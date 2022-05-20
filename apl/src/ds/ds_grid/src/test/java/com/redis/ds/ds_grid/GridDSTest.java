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

package com.redis.ds.ds_grid;

import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.ds.DSException;
import com.redis.foundation.io.DataDocXML;
import com.redis.foundation.io.DataGridCSV;
import com.redis.foundation.io.DataGridConsole;
import com.redis.ds.ds_content.ContentClean;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Optional;

/**
 * This Junit class will exercise the Grid data source features.
 */
public class GridDSTest
{
    private final String GDS_PROPERTY_PREFIX = "gds";

    private AppCtx mAppCtx;

    @Before
    public void setup()
    {
        HashMap<String,Object> hmProperties = new HashMap<>();
        hmProperties.put(GDS_PROPERTY_PREFIX + ".host_name", "localhost");
        hmProperties.put(GDS_PROPERTY_PREFIX + ".port_number", 4455);
        hmProperties.put(GDS_PROPERTY_PREFIX + ".application_name", "GraphDS");
        hmProperties.put(GDS_PROPERTY_PREFIX + ".database_id", 0);
        hmProperties.put(GDS_PROPERTY_PREFIX + ".operation_timeout", 60);
        hmProperties.put(GDS_PROPERTY_PREFIX + ".encrypt_password", "1c518a1e-be3b-4ff0-8478-f319b887dca0");
        mAppCtx = new AppCtx(hmProperties);
    }

    // Handy method for cleaning CSV files with non-ASCII encodings.
    public void cleanGridFiles()
    {
        ContentClean contentClean = new ContentClean(mAppCtx);
        try
        {
//            contentClean.readCleanWriteFile("data/dg-hr-tiny-records.csv", "data/dg-hr-tiny-records-clean.csv");
//            contentClean.readCleanWriteFile("data/dg-hr-small-records.csv", "data/dg-hr-small-records-clean.csv");
//            contentClean.readCleanWriteFile("data/dg-hr-1000-records.csv", "data/dg-hr-1000-records-clean.csv");
//            contentClean.readCleanWriteFile("data/dg-hr-all-records.csv", "data/dg-hr-all-records-clean.csv");
            contentClean.readCleanWriteFile("data/job_data.csv", "data/job_data_clean.csv");
        }
        catch (IOException e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    // Handy method that demonstrates how to transforms columns in a CSV file.
    public void transformREGrid()
    {
        String propertyName;
        DataDoc curDoc, newDoc;

        DataGridCSV dataGridCSV = new DataGridCSV();
        try
        {
            Optional<DataGrid> optDataGrid = dataGridCSV.load("data/re-985-records.csv", true);
            Assert.assertTrue(optDataGrid.isPresent());
            DataGrid dataGrid1 = optDataGrid.get();
            DataDoc dataDoc = dataGrid1.getColumns();
            DataDoc schemaDoc = new DataDoc("Real Estate Transactions");
            schemaDoc.add(new DataItem.Builder().name("property_name").title("Property Name").build());
            for (DataItem dataItem : dataDoc.getItems())
                schemaDoc.add(dataItem);
            DataGrid dataGrid2 = new DataGrid(schemaDoc);
            int rowCount = dataGrid1.rowCount();
            for (int row = 0; row < rowCount; row++)
            {
                curDoc = dataGrid1.getRowAsDoc(row);
                newDoc = new DataDoc(schemaDoc);
                propertyName = String.format("%s %d", curDoc.getValueByName("type"), row);
                newDoc.setValueByName("property_name", propertyName);
                for (DataItem dataItem : curDoc.getItems())
                    newDoc.setValueByName(dataItem.getName(), dataItem.getValue());
                dataGrid2.addRow(newDoc);
            }
            dataGridCSV.save(dataGrid2, "data/ret-985-records.csv", true);
        }
        catch (IOException e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    public void transformHRWithJobData()
    {
        DataDoc hrDoc, jobDoc;
        String fullName, genderInitial, genderName, industryName;

        GridDS hrGridDS = new GridDS(mAppCtx);
        GridDS hreGridDS = new GridDS(mAppCtx);
        GridDS jobGridDS = new GridDS(mAppCtx);
        try
        {
            hrGridDS.loadSchema("data/hr-records.xml");
            hrGridDS.loadData("data/hr_records_10k.csv", false);
            hrGridDS.setName("Human Resource Employees");
            DataGrid hrDataGrid = hrGridDS.getDataGrid();

            jobGridDS.loadSchema("data/job_data.xml");
            jobGridDS.loadData("data/job_data.csv", false);
            jobGridDS.setName("Job Data");
            DataGrid jobDataGrid = jobGridDS.getDataGrid();

            hreGridDS.loadSchema("data/hr_employees.xml");
            DataGrid hreDataGrid = hreGridDS.getDataGrid();

            int jobOffset = 1;
            int jobRowCount = jobDataGrid.rowCount();
            int hrRowCount = hrDataGrid.rowCount();
            for (int row = 0; row < hrRowCount; row++)
            {
                jobDoc = jobDataGrid.getRowAsDoc(jobOffset);
                hrDoc = hrDataGrid.getRowAsDoc(row);
                fullName = String.format("%s %s", hrDoc.getValueByName("first_name"), hrDoc.getValueByName("last_name"));
                genderInitial = hrDoc.getValueByName("gender");
                if (genderInitial.charAt(0) == 'F')
                    genderName = "Female";
                else
                    genderName = "Male";

                hreDataGrid.newRow();
                hreDataGrid.setValueByName("employee_id", hrDoc.getValueByName("employee_id"));
                hreDataGrid.setValueByName("full_name", fullName);
                hreDataGrid.setValueByName("gender", genderName);
                hreDataGrid.setValueByName("email_address", hrDoc.getValueByName("email_address"));
                hreDataGrid.setValueByName("father_name", hrDoc.getValueByName("father_name"));
                hreDataGrid.setValueByName("mother_name", hrDoc.getValueByName("mother_name"));
                hreDataGrid.setValueByName("position_title", jobDoc.getValueByName("position"));
                hreDataGrid.setValueByName("office_location", jobDoc.getValueByName("location"));
                industryName = jobDoc.getValueByName("industry");
                if ((StringUtils.isEmpty(industryName)) || (StringUtils.equalsIgnoreCase(industryName, "Unknown")))
                    industryName = "Manufacturing";
                hreDataGrid.setValueByName("industry_focus", industryName);
                hreDataGrid.setValueByName("job_description", jobDoc.getValueByName("job_description"));
                hreDataGrid.setValueByName("date_of_birth", hrDoc.getValueByName("date_of_birth"));
                hreDataGrid.setValueByName("age_in_years", hrDoc.getValueByName("age_in_years"));
                hreDataGrid.setValueByName("date_of_joining", hrDoc.getValueByName("date_of_joining"));
                hreDataGrid.setValueByName("age_in_company", hrDoc.getValueByName("age_in_company"));
                hreDataGrid.setValueByName("salary", hrDoc.getValueByName("salary"));
                hreDataGrid.setValueByName("last_percent_hike", hrDoc.getValueByName("last_percent_hike"));
                hreDataGrid.setValueByName("social_security_number", hrDoc.getValueByName("ssn"));
                hreDataGrid.setValueByName("phone_number", hrDoc.getValueByName("phone_number"));
                hreDataGrid.setValueByName("place_name", hrDoc.getValueByName("place_name"));
                hreDataGrid.setValueByName("county", hrDoc.getValueByName("county"));
                hreDataGrid.setValueByName("city", hrDoc.getValueByName("city"));
                hreDataGrid.setValueByName("state", hrDoc.getValueByName("state"));
                hreDataGrid.setValueByName("zip", hrDoc.getValueByName("zip"));
                hreDataGrid.setValueByName("region", hrDoc.getValueByName("region"));
                hreDataGrid.setValueByName("user_name", hrDoc.getValueByName("user_name"));
                hreDataGrid.setValueByName("password", String.format("%s%d", hrDoc.getValueByName("last_name"), row));
                hreDataGrid.addRow();

                if (jobOffset < jobRowCount-1)
                    jobOffset++;
                else
                    jobOffset = 0;
            }
            DataGridCSV dataGridCSV = new DataGridCSV();
            dataGridCSV.save(hreDataGrid, "data/hr_employees_10k.csv", false);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    public boolean gridFirstEmployeeId(DataGrid aDataGrid, String anEmployeeId)
    {
        DataDoc dataDoc = aDataGrid.getRowAsDoc(0);
        String rowEmployeeId = dataDoc.getValueByName("employee_id");

        return StringUtils.equals(rowEmployeeId, anEmployeeId);
    }

    public boolean gridLastEmployeeId(DataGrid aDataGrid, String anEmployeeId)
    {
        DataDoc dataDoc = aDataGrid.getRowAsDoc(aDataGrid.rowCount()-1);
        String rowEmployeeId = dataDoc.getValueByName("employee_id");

        return StringUtils.equals(rowEmployeeId, anEmployeeId);
    }

    public boolean gridContainsEmployeeIds(DataGrid aDataGrid, String... anEmployeeIds)
    {
        DataDoc dataDoc;
        boolean isMatched;
        String rowEmployeeId;

        int rowCount = aDataGrid.rowCount();
        if (rowCount != anEmployeeIds.length)
            return false;
        isMatched = false;
        for (int row = 0; row < rowCount; row++)
        {
            isMatched = false;
            dataDoc = aDataGrid.getRowAsDoc(row);
            rowEmployeeId = dataDoc.getValueByName("employee_id");
            for (String employeeId : anEmployeeIds)
            {
                if (StringUtils.equals(rowEmployeeId, employeeId))
                {
                    isMatched = true;
                    break;
                }
            }
            if (! isMatched)
                break;
        }

        return isMatched;
    }

    public void exerciseSmartSheetCriteria()
    {
        GridDS gridDS = new GridDS(mAppCtx);
        try
        {
            gridDS.loadData("data/smartsheet_projects.csv", true);
            gridDS.setName("PS Project Records");

            DSCriteria dsCriteria = new DSCriteria("Active Projects Criteria");
            dsCriteria.add("hours", Data.Operator.GREATER_THAN, 0.0);
            DataGrid dataGrid = gridDS.fetch(dsCriteria, 0, 5000);

            DataGridConsole dataGridConsole = new DataGridConsole();
            PrintWriter printWriter = new PrintWriter(System.out, true);
            dataGridConsole.write(dataGrid, printWriter, dataGrid.getName(), 40, 1);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    public void exerciseCriteria()
    {
        GridDS gridDS = new GridDS(mAppCtx);
        try
        {
            gridDS.loadData("data/hr-1000-records.csv", true);
            gridDS.setName("Human Resources Records");
            gridDS.setPrimaryKey("employee_id");

            DSCriteria dsCriteria = new DSCriteria("Human Resources Criteria");
            dsCriteria.addFeature(Data.FEATURE_DS_OFFSET, 0);
            dsCriteria.addFeature(Data.FEATURE_DS_LIMIT, 10);
            dsCriteria.add("father_name", Data.Operator.EQUAL, "Phil Hafner");
            DataGrid dataGrid = gridDS.fetch(dsCriteria);
            String[] employeeIds = {"207808"};
            Assert.assertTrue(gridContainsEmployeeIds(dataGrid, employeeIds));

            dsCriteria.reset();
            dsCriteria.add("salary", Data.Operator.BETWEEN_INCLUSIVE, 199583, 199907);
            dataGrid = gridDS.fetch(dsCriteria);
            Assert.assertTrue(gridContainsEmployeeIds(dataGrid, "740463", "661572", "488166", "304092"));

            dsCriteria.reset();
            dsCriteria.add("age_in_company", Data.Operator.GREATER_THAN, 37.0);
            dsCriteria.add("year_of_joining", Data.Operator.LESS_THAN, 2020);
            dsCriteria.add("half_of_joining", Data.Operator.EQUAL, "H1");
            dataGrid = gridDS.fetch(dsCriteria);
            Assert.assertTrue(gridContainsEmployeeIds(dataGrid, "845835", "414971"));

            dsCriteria.reset();
            dsCriteria.add("phone_number", Data.Operator.STARTS_WITH, "803-");
            dsCriteria.add("place_name", Data.Operator.EQUAL, "Columbia");
            dataGrid = gridDS.fetch(dsCriteria);
            Assert.assertTrue(gridContainsEmployeeIds(dataGrid, "914698", "304731"));

            dsCriteria.reset();
            dsCriteria.setCaseSensitive(false);
            dsCriteria.add("phone_number", Data.Operator.STARTS_WITH, "803-");
            dsCriteria.add("place_name", Data.Operator.EQUAL, "COLUMBIA");
            dataGrid = gridDS.fetch(dsCriteria);
            Assert.assertTrue(gridContainsEmployeeIds(dataGrid, "914698", "304731"));

            dsCriteria.reset();
            dsCriteria.addFeature(Data.FEATURE_DS_LIMIT, 1000);
            dsCriteria.add("employee_id", Data.Operator.GREATER_THAN, 100000);
            dsCriteria.add("employee_id", Data.Operator.SORT, Data.Order.ASCENDING.name());
            dataGrid = gridDS.fetch(dsCriteria);
            Assert.assertEquals(1000, dataGrid.rowCount());
            Assert.assertTrue(gridFirstEmployeeId(dataGrid, "111282"));
            Assert.assertTrue(gridLastEmployeeId(dataGrid, "996223"));

            dsCriteria.reset();
            dsCriteria.addFeature(Data.FEATURE_DS_LIMIT, 1000);
            dsCriteria.add("employee_id", Data.Operator.GREATER_THAN, 100000);
            dsCriteria.add("employee_id", Data.Operator.SORT, Data.Order.DESCENDING.name());
            dataGrid = gridDS.fetch(dsCriteria);
            Assert.assertEquals(1000, dataGrid.rowCount());
            Assert.assertTrue(gridFirstEmployeeId(dataGrid, "996223"));
            Assert.assertTrue(gridLastEmployeeId(dataGrid, "111282"));

            dsCriteria.reset();
            dsCriteria.addFeature(Data.FEATURE_DS_LIMIT, 1000);
            dsCriteria.add("last_name", Data.Operator.SORT, Data.Order.ASCENDING.name());
            dataGrid = gridDS.fetch(dsCriteria);
            Assert.assertEquals(1000, dataGrid.rowCount());
            Assert.assertTrue(gridFirstEmployeeId(dataGrid, "518577"));
            Assert.assertTrue(gridLastEmployeeId(dataGrid, "242757"));

            dsCriteria.reset();
            dsCriteria.addFeature(Data.FEATURE_DS_LIMIT, 1000);
            dsCriteria.add("last_name", Data.Operator.SORT, Data.Order.DESCENDING.name());
            dataGrid = gridDS.fetch(dsCriteria);
            Assert.assertEquals(1000, dataGrid.rowCount());
            Assert.assertTrue(gridFirstEmployeeId(dataGrid, "242757"));
            Assert.assertTrue(gridLastEmployeeId(dataGrid, "518577"));

            dsCriteria.reset();
            dsCriteria.add("last_name", Data.Operator.SORT, Data.Order.DESCENDING.name());
            dataGrid = gridDS.fetch(dsCriteria, 0, 10);
            Assert.assertEquals(10, dataGrid.rowCount());
            Assert.assertTrue(gridFirstEmployeeId(dataGrid, "242757"));

            dsCriteria.reset();
            dsCriteria.add("last_name", Data.Operator.SORT, Data.Order.DESCENDING.name());
            dataGrid = gridDS.fetch(dsCriteria, 10, 20);
            Assert.assertEquals(20, dataGrid.rowCount());
            Assert.assertTrue(gridFirstEmployeeId(dataGrid, "197264"));
        }
        catch (DSException | IOException e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    public void exerciseCRUD()
    {
        GridDS gridDS = new GridDS(mAppCtx);
        try
        {
            gridDS.loadData("data/hr-1000-records.csv", true);
            gridDS.setName("Human Resources Records");
            gridDS.setPrimaryKey("employee_id");

            DataGrid csvDataGrid = gridDS.getDataGrid();
            DSCriteria dsCriteria = new DSCriteria("Human Resources Criteria");
            DataGrid dataGrid = gridDS.fetch(dsCriteria, 0, 10);
            DataDoc dataDoc = dataGrid.getRowAsDoc(5);
            dataDoc.setValueByName("employee_id", 111111);
            dataDoc.setValueByName("first_name", "Al");
            dataDoc.setValueByName("last_name", "Cole");
            dataDoc.setValueByName("father_name", "Al Cole");
            dataGrid.insertRow(5, dataDoc);
            gridDS.setDatGrid(dataGrid);
            dsCriteria.add("father_name", Data.Operator.EQUAL, "Al Cole");
            dataGrid = gridDS.fetch(dsCriteria);
            Assert.assertTrue(gridContainsEmployeeIds(dataGrid, "111111"));

            gridDS.setDatGrid(csvDataGrid);
            dsCriteria.reset();
            dataGrid = gridDS.fetch(dsCriteria, 0, 10);
            dataDoc = dataGrid.getRowAsDoc(5);
            dataDoc.setValueByName("first_name", "Al");
            dataDoc.setValueByName("last_name", "Cole");
            dataDoc.setValueByName("father_name", "Al Cole Sr.");
            gridDS.update(dataDoc);
            dsCriteria.add("father_name", Data.Operator.EQUAL, "Al Cole Sr.");
            dataGrid = gridDS.fetch(dsCriteria);
            Assert.assertEquals(1, dataGrid.rowCount());

            gridDS.setDatGrid(csvDataGrid);
            dsCriteria.reset();
            dataGrid = gridDS.fetch(dsCriteria, 0, 10);
            dataDoc = dataGrid.getRowAsDoc(5);
            DataDoc dataDoc2 = new DataDoc(dataDoc.getName());
            dataDoc2.add(new DataItem(dataDoc.getItemByName("employee_id")));
            dataDoc2.add(new DataItem(dataDoc.getItemByName("first_name")));
            dataDoc2.add(new DataItem(dataDoc.getItemByName("last_name")));
            dataDoc2.add(new DataItem(dataDoc.getItemByName("father_name")));
            dataDoc2.setValueByName("first_name", "Al");
            dataDoc2.setValueByName("last_name", "Cole");
            dataDoc2.setValueByName("father_name", "Al Cole Sr.");
            DataDoc dataDoc3 = gridDS.loadApplyUpdate(dataDoc2);
            Assert.assertEquals(dataDoc.count(), dataDoc3.count());
            dsCriteria.add("father_name", Data.Operator.EQUAL, "Al Cole Sr.");
            dataGrid = gridDS.fetch(dsCriteria);
            Assert.assertEquals(1, dataGrid.rowCount());

            gridDS.setDatGrid(csvDataGrid);
            dsCriteria.reset();
            dataGrid = gridDS.fetch(dsCriteria, 0, 10);
            dataDoc = dataGrid.getRowAsDoc(5);
            gridDS.delete(dataDoc);
            Assert.assertFalse(gridContainsEmployeeIds(dataGrid, dataDoc.getValueByName("employee_id")));

            gridDS.setDatGrid(csvDataGrid);
            dsCriteria.reset();
            dataGrid = gridDS.fetch(dsCriteria, 0, 10);
            dataGrid.setValueByRowName(4, "first_name", "Al");
            dataGrid.setValueByRowName(4, "last_name", "Cole");
            dataGrid.setValueByRowName(4, "father_name", "Al Cole Sr.");
            dataDoc = dataGrid.getRowAsDoc(4);
            Assert.assertEquals(dataDoc.getValueByName("father_name"), "Al Cole Sr.");
        }
        catch (DSException | IOException e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    public void exerciseSearch()
    {
        GridDS gridDS = new GridDS(mAppCtx);
        try
        {
            gridDS.loadSchema("data/hr-records.xml");
            gridDS.loadData("data/hr-records.csv", false);
            gridDS.setName("Human Resources Records");

            DataGrid dataGrid = gridDS.search("mar");
            Assert.assertEquals(16, dataGrid.rowCount());
            dataGrid = gridDS.search("jamar");
            Assert.assertEquals(1, dataGrid.rowCount());
            DataGridConsole dataGridConsole = new DataGridConsole();
            PrintWriter printWriter = new PrintWriter(System.out, true);
            dataGridConsole.write(dataGrid, printWriter, dataGrid.getName());
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    public void exerciseSuggest()
    {
        GridDS gridDS = new GridDS(mAppCtx);
        try
        {
            gridDS.loadSchema("data/hr-records.xml");
            gridDS.loadData("data/hr-records.csv", false);
            gridDS.setName("Human Resources Records");

            DataGrid dataGrid = gridDS.suggest("Jonas Peterkin");
            Assert.assertEquals(1, dataGrid.rowCount());
            dataGrid = gridDS.suggest("M");
            Assert.assertEquals(5, dataGrid.rowCount());
            DataGridConsole dataGridConsole = new DataGridConsole();
            PrintWriter printWriter = new PrintWriter(System.out, true);
            dataGridConsole.write(dataGrid, printWriter, dataGrid.getName());

            dataGrid = gridDS.suggest("TY", Data.Operator.CONTAINS, 5);
            Assert.assertEquals(5, dataGrid.rowCount());
            dataGridConsole.write(dataGrid, printWriter, dataGrid.getName());
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    public void exerciseStatistics()
    {
        GridDS gridDS = new GridDS(mAppCtx);
        try
        {
            gridDS.loadData("data/hr-1000-records.csv", true);
            gridDS.setName("Human Resources Records");
            gridDS.setPrimaryKey("employee_id");

            DataGrid dataGrid = gridDS.fetch();
            // https://www.baeldung.com/apache-commons-math
            DescriptiveStatistics descriptiveStatistics = dataGrid.getDescriptiveStatistics("salary");
            double min = descriptiveStatistics.getMin();
            double max = descriptiveStatistics.getMax();
            double mean = descriptiveStatistics.getMean();
            double median = descriptiveStatistics.getPercentile(50);
            double standardDeviation = descriptiveStatistics.getStandardDeviation();
            System.out.println("--- Descriptive Statistics ---");
            System.out.printf("min = %.2f, max = %.2f, mean = %.2f, median = %.2f, std = %.2f%n", min, max, mean, median, standardDeviation);
        }
        catch (DSException | IOException e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    public void exerciseConsole()
    {
        GridDS gridDS = new GridDS(mAppCtx);
        try
        {
            gridDS.loadData("data/hr-1000-records.csv", true);
            gridDS.setName("Human Resources Records");
            gridDS.setPrimaryKey("employee_id");

            DataGrid dataGrid = gridDS.fetch(0,10);
            DataGridConsole dataGridConsole = new DataGridConsole();
            PrintWriter printWriter = new PrintWriter(System.out, true);
            dataGridConsole.write(dataGrid, printWriter, dataGrid.getName());
        }
        catch (DSException | IOException e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    public void exerciseDataGridFromProperties()
    {
        DataGrid dataGrid = mAppCtx.dataGridFromProperties();
        DataGridConsole dataGridConsole = new DataGridConsole();
        PrintWriter printWriter = new PrintWriter(System.out, true);
        dataGridConsole.write(dataGrid, printWriter, dataGrid.getName());
    }

    public void saveStandardSchema()
    {
        GridDS gridDS = new GridDS(mAppCtx);
        try
        {
            gridDS.loadSchema("data/hr-records.xml");
            gridDS.loadData("data/hr-records.csv", false);
            gridDS.setName("Human Resources Records");
            gridDS.setPrimaryKey("employee_id");
            DataGrid schemaGrid = Data.schemaDocToDataGrid(gridDS.getSchema(), false);
            DataDocXML dataDocXML = new DataDocXML(schemaGrid.getColumns());
            dataDocXML.save("data/standard_schema.xml");
            DataGridConsole dataGridConsole = new DataGridConsole();
            dataGridConsole.setFormattedFlag(true);
            PrintWriter printWriter = new PrintWriter(System.out, true);
            dataGridConsole.write(schemaGrid, printWriter, schemaGrid.getName());
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    public void loadDataWithHeaderSaveSchema()
    {
        GridDS gridDS = new GridDS(mAppCtx);
        try
        {
            gridDS.loadData("data/product_electronics.csv", true);
            gridDS.setName("Electronic Products");
            gridDS.setPrimaryKey("sku");
            gridDS.saveSchema("data/product_electronics.xml");
            DataGridConsole dataGridConsole = new DataGridConsole();
            dataGridConsole.setFormattedFlag(true);
            PrintWriter printWriter = new PrintWriter(System.out, true);
            dataGridConsole.write(gridDS.getDataGrid(), printWriter, gridDS.getName());
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    /* This logic assumes that 'father_name' has 'isSuggest' enabled and you will
       be disabling that item and assigning 'mother_name' as 'isSuggest'.
    */
    public void exerciseDataGridSchema()
    {
        DataDoc rowDoc;

        GridDS gridDS = new GridDS(mAppCtx);
        try
        {
            gridDS.loadSchema("data/hr-records.xml");
            gridDS.loadData("data/hr-records.csv", false);
            gridDS.setName("Human Resources Records");
            gridDS.setPrimaryKey("employee_id");
            DataGrid schemaGrid = Data.schemaDocToDataGrid(gridDS.getSchema(), false);
            DataGridConsole dataGridConsole = new DataGridConsole();
            dataGridConsole.setFormattedFlag(true);
            PrintWriter printWriter = new PrintWriter(System.out, true);
            dataGridConsole.write(schemaGrid, printWriter, "Before Update - " + schemaGrid.getName());
            int rowCount = schemaGrid.rowCount();
            for (int row = 0; row < rowCount; row++)
            {
                rowDoc = schemaGrid.getRowAsDoc(row);
                if (rowDoc.getValueByName("item_name").equals("father_name"))
                {
                    rowDoc.setValueByName("isSuggest", false);
                    Assert.assertTrue(gridDS.updateSchema(rowDoc));
                }
                else if (rowDoc.getValueByName("item_name").equals("mother_name"))
                {
                    rowDoc.setValueByName("isSuggest", true);
                    Assert.assertTrue(gridDS.updateSchema(rowDoc));
                }
            }
            schemaGrid = Data.schemaDocToDataGrid(gridDS.getSchema(), false);
            dataGridConsole.write(schemaGrid, printWriter, "After Update - " + schemaGrid.getName());
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    public void exerciseAppStudio()
    {
        DataDoc rowDoc;

        GridDS gridDS = new GridDS(mAppCtx);
        try
        {
            gridDS.loadSchema("data/hr_employee_records_1k.xml");
            gridDS.loadData("data/hr_employee_records_1k.csv", false);
            gridDS.setName("Employee Records");
            gridDS.setPrimaryKey("employee_id");

            DataGridConsole dataGridConsole = new DataGridConsole();
            dataGridConsole.setFormattedFlag(true);
            PrintWriter printWriter = new PrintWriter(System.out, true);

            DSCriteria dsCriteria = new DSCriteria("Human Resources Employee Criteria");
            dsCriteria.add("salary", Data.Operator.LESS_THAN_FIELD, "zip");
            DataGrid dataGrid = gridDS.fetch(dsCriteria);
            dataGridConsole.write(dataGrid, printWriter, dsCriteria.getName());

            dsCriteria.reset();
            dsCriteria.add("region", Data.Operator.IN, "West", "South");
            dataGrid = gridDS.fetch(dsCriteria);
            dataGridConsole.write(dataGrid, printWriter, dsCriteria.getName());

            DataGrid schemaGrid = Data.schemaDocToDataGrid(gridDS.getSchema(), false);
            dataGridConsole.write(schemaGrid, printWriter, "Before Update - " + schemaGrid.getName());
            int rowCount = schemaGrid.rowCount();
            for (int row = 0; row < rowCount; row++)
            {
                rowDoc = schemaGrid.getRowAsDoc(row);
                if (rowDoc.getValueByName("item_name").equals("full_name"))
                {
                    rowDoc.setValueByName(Data.FEATURE_IS_SUGGEST, false);
                    Assert.assertTrue(gridDS.updateSchema(rowDoc));
                }
                else if (rowDoc.getValueByName("item_name").equals("mother_name"))
                {
                    rowDoc.setValueByName(Data.FEATURE_IS_SEARCH, true);
                    rowDoc.setValueByName(Data.FEATURE_IS_SUGGEST, true);
                    Assert.assertTrue(gridDS.updateSchema(rowDoc));
                }
            }
            schemaGrid = Data.schemaDocToDataGrid(gridDS.getSchema(), false);
            dataGridConsole.write(schemaGrid, printWriter, "After Update - " + schemaGrid.getName());

            DataGrid analyzeGrid = gridDS.analyze(5);
            dataGridConsole.write(analyzeGrid, printWriter, analyzeGrid.getName(), 40, 1);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    @Test
    public void exercise()
    {
        exerciseSmartSheetCriteria();
        exerciseAppStudio();
        exerciseCriteria();
        exerciseSearch();
        exerciseSuggest();
        exerciseCRUD();
        exerciseStatistics();
        exerciseConsole();
        exerciseDataGridFromProperties();
        exerciseDataGridSchema();
    }

    @After
    public void cleanup()
    {
    }
}
