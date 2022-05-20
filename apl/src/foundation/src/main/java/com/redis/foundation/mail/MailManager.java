/*
 * NorthRidge Software, LLC - Copyright (c) 2015.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.redis.foundation.mail;

import com.redis.foundation.app.AppCtx;
import com.redis.foundation.app.CfgMgr;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.std.FCException;
import com.redis.foundation.std.StrUtl;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.io.DataDocConsole;
import com.redis.foundation.io.DataGridConsole;
import freemarker.template.Configuration;
import freemarker.template.DefaultObjectWrapper;
import freemarker.template.Template;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * The MailManager is responsible capturing messages and
 * sending them to email recipients based on configuration
 * settings. The class offers a flexible template approach
 * to message generation via Freemarker Templates.
 *
 * @see <a href="https://crunchify.com/java-mailapi-example-send-an-email-via-gmail-smtp/">Java MailAPI Example – Send an Email via GMail SMTP (TLS Authentication)</a>
 * @see <a href="https://stackoverflow.com/questions/43406528/javamail-api-username-and-password-not-accepted-gmail">JavaMail API : Username and Password not accepted (Gmail)</a>
 * @see <a href="http://javamail.kenai.com/nonav/javadocs/com/sun/mail/smtp/package-summary.html">Mail Reference</a>
 * @see <a href="http://crunchify.com/java-mailapi-example-send-an-email-via-gmail-smtp/">GMail SMTP (TLS Authentication)</a>
 * @see <a href="http://freemarker.org/">Freemarker Template</a>
 */
public class MailManager
{
    private AppCtx mAppCtx;
    private CfgMgr mCfgMgr;
    private DataGrid mDataGrid;
    private Session mMailSession;
    private Configuration mConfiguration;

    public MailManager(AppCtx anAppCtx)
    {
        mAppCtx = anAppCtx;
        mCfgMgr = new CfgMgr(anAppCtx, Mail.CFG_PROPERTY_PREFIX);
        mDataGrid = new DataGrid("Mail Manager", schemaServiceMailBag());
    }

    public MailManager(AppCtx anAppCtx, DataDoc aDoc)
    {
        mAppCtx = anAppCtx;
        mCfgMgr = new CfgMgr(anAppCtx, Mail.CFG_PROPERTY_PREFIX);
        mDataGrid = new DataGrid("Mail Manager", aDoc);
    }

    public MailManager(AppCtx anAppCtx, String aPropertyPrefix)
    {
        mAppCtx = anAppCtx;
        mCfgMgr = new CfgMgr(anAppCtx, aPropertyPrefix);
        mDataGrid = new DataGrid("Mail Manager", schemaServiceMailBag());
    }

    /**
     * Returns the configuration property prefix string.
     *
     * @return Property prefix string.
     */
    public String getCfgPropertyPrefix()
    {
        return mCfgMgr.getPrefix();
    }

    /**
     * Assigns the configuration property prefix to the document data source.
     *
     * @param aPropertyPrefix Property prefix.
     */
    public void setCfgPropertyPrefix(String aPropertyPrefix)
    {
        mCfgMgr.setCfgPropertyPrefix(aPropertyPrefix);
    }

    /**
     * Convenience method that returns the value of an application
     * manager configuration property using the concatenation of
     * the property prefix and suffix values.
     *
     * @param aSuffix Property name suffix.
     * @return Matching property value.
     */
    public String getCfgString(String aSuffix)
    {
        return mCfgMgr.getString(aSuffix);
    }

    /**
     * Convenience method that returns the value of an application
     * manager configuration property using the concatenation of
     * the property prefix and suffix values.  If the property is
     * not found, then the default value parameter will be returned.
     *
     * @param aSuffix Property name suffix.
     * @param aDefaultValue Default value.
     *
     * @return Matching property value or the default value.
     */
    public String getCfgString(String aSuffix, String aDefaultValue)
    {
        return mCfgMgr.getString(aSuffix, aDefaultValue);
    }

    /**
     * Returns a typed value for the property name identified
     * or the default value (if unmatched).
     *
     * @param aSuffix Property name suffix.
     * @param aDefaultValue Default value to return if property
     *                      name is not matched.
     *
     * @return Value of the property.
     */
    public int getCfgInteger(String aSuffix, int aDefaultValue)
    {
        return mCfgMgr.getInteger(aSuffix, aDefaultValue);
    }

    /**
     * Returns <i>true</i> if the application manager configuration
     * property value evaluates to <i>true</i>.
     *
     * @param aSuffix Property name suffix.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isCfgStringTrue(String aSuffix)
    {
        return mCfgMgr.isStringTrue(aSuffix);
    }

    /**
     * Performs a property lookup for the from address.
     *
     * @return Email address from property file.
     *
     * @throws FCException Property is undefined.
     */
    public String lookupFromAddress()
        throws FCException
    {
        String propertyName = "address_from";
        String mailAddressFrom = getCfgString(propertyName);
        if (StringUtils.isEmpty(mailAddressFrom))
        {
            String msgStr = String.format("Mail Manager property '%s' is undefined.", mCfgMgr.getPrefix() + "." + propertyName);
            throw new FCException(msgStr);
        }

        return mailAddressFrom;
    }

    /**
     * Convenience method that assigns the recipient email address to an array
     * list.
     *
     * @param anEmailAddress Single email address.
     *
     * @return Array list of email address strings.
     */
    public ArrayList<String> createRecipientList(String anEmailAddress)
    {
        ArrayList<String> recipientList = new ArrayList<String>();
        recipientList.add(anEmailAddress);

        return recipientList;
    }

    /**
     * Convenience method that assigns the recipient email addresses
     * defined in the application property file to an array list.
     *
     * @return Array list of email address strings.
     *
     * @throws FCException When missing properties are detected
     */
    public ArrayList<String> createRecipientList()
        throws FCException
    {
        ArrayList<String> recipientList = new ArrayList<String>();

        String propertyName = "address_to";
        if (mAppCtx.isPropertyMultiValue(getCfgString(propertyName)))
        {
            String[] addressToArray = mAppCtx.getStringArray(getCfgString(propertyName));
            for (String mailAddressTo : addressToArray)
                recipientList.add(mailAddressTo);
        }
        else
        {
            String mailAddressTo = getCfgString(propertyName);
            if (StringUtils.isEmpty(mailAddressTo))
            {
                String msgStr = String.format("Mail Manager property '%s' is undefined.", mCfgMgr.getPrefix() + "." + propertyName);
                throw new FCException(msgStr);
            }
        }

        return recipientList;
    }

    /**
     * Convenience method that assigns a single attachment to an array
     * list.
     *
     * @param anAttachmentPathFileName Attachment path file name.
     *
     * @return Array list of attachment path/file names.
     */
    public ArrayList<String> createAttachmentList(String anAttachmentPathFileName)
    {
        ArrayList<String> attachmentPathFileList = new ArrayList<String>();
        attachmentPathFileList.add(anAttachmentPathFileName);

        return attachmentPathFileList;
    }

    /**
     * This method will create a mail data document with one item for a
     * message description.
     *
     * @return Data document instance.
     */
    public DataDoc schemaMailDocument()
    {
        DataDoc dataDoc = new DataDoc("Application Mail Manager");

        DataItem dataItem = new DataItem.Builder().name("msg_description").title("Message Description").build();
        dataDoc.add(dataItem);

        return dataDoc;
    }

    /**
     * This method will create a mail document of items suitable for
     * capturing a table of messages that could describe the result
     * of a batch operation like a connector service.
     *
     * @return Data document instance.
     */
    public DataDoc schemaServiceMailBag()
    {
        DataDoc dataDoc = new DataDoc("Application Service Mail Manager");

        dataDoc.add(new DataItem.Builder().type(Data.Type.DateTime).name("msg_ts").title("Message Timestamp").defaultValue(Data.VALUE_DATETIME_TODAY).build());
        dataDoc.add(new DataItem.Builder().name("msg_operation").title("Message Operation").build());
        dataDoc.add(new DataItem.Builder().name("msg_status").title("Message Status").defaultValue(Mail.STATUS_SUCCESS).build());
        dataDoc.add(new DataItem.Builder().name("msg_description").title("Message Description").defaultValue(Mail.MESSAGE_NONE).build());
        dataDoc.add(new DataItem.Builder().name("msg_detail").title("Message Detail").defaultValue(Mail.MESSAGE_NONE).build());

        DataItem dataItem = new DataItem.Builder().name("msg_operation").title("Message Operation").build();
        dataItem.enableFeature(Data.FEATURE_IS_REQUIRED);
        dataDoc.add(dataItem);

        dataDoc.resetValuesWithDefaults();

        return dataDoc;
    }

    private void initialize()
        throws FCException
    {
        Logger appLogger = mAppCtx.getLogger(this, "initialize");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

        if (mMailSession == null)
        {
            String propertyName = "account_name";
            String mailAccountName = getCfgString(propertyName);
            if (StringUtils.isEmpty(mailAccountName))
            {
                String msgStr = String.format("Mail Manager property '%s' is undefined.", mCfgMgr.getPrefix() + "." + propertyName);
                appLogger.error(msgStr);
                throw new FCException(msgStr);
            }
            propertyName = "account_password";
            String mailAccountPassword = getCfgString(propertyName);
            if (StringUtils.isEmpty(mailAccountPassword))
            {
                String msgStr = String.format("Mail Manager property '%s' is undefined.", mCfgMgr.getPrefix() + "." + propertyName);
                appLogger.error(msgStr);
                throw new FCException(msgStr);
            }
            else
            {
                if (StrUtl.isHidden(mailAccountPassword))
                    mailAccountPassword = StrUtl.recoverPassword(mailAccountPassword);
            }

            MailAuthenticator mailAuthenticator = new MailAuthenticator(mailAccountName, mailAccountPassword);

            propertyName = "smtp_host";
            String smtpHostName = getCfgString(propertyName);
            if (StringUtils.isEmpty(smtpHostName))
            {
                String msgStr = String.format("Mail Manager property '%s' is undefined.", mCfgMgr.getPrefix() + "." + propertyName);
                appLogger.error(msgStr);
                throw new FCException(msgStr);
            }
            propertyName = "smtp_port";
            String smtpPortNumber = getCfgString(propertyName);
            if (StringUtils.isEmpty(smtpHostName))
            {
                String msgStr = String.format("Mail Manager property '%s' is undefined.", mCfgMgr.getPrefix() + "." + propertyName);
                appLogger.error(msgStr);
                throw new FCException(msgStr);
            }

            Properties systemProperties = new Properties();

            systemProperties.setProperty("mail.smtp.submitter", mailAccountName);
            systemProperties.setProperty("mail.smtp.host", smtpHostName);
            systemProperties.setProperty("mail.smtp.port", smtpPortNumber);
            if (isCfgStringTrue("authn_enabled"))
            {
                systemProperties.setProperty("mail.smtp.auth", "true");
                systemProperties.setProperty("mail.smtp.starttls.enable", "true");
                mMailSession = Session.getInstance(systemProperties, mailAuthenticator);
            }
            else
                mMailSession = Session.getInstance(systemProperties);

            mConfiguration = new Configuration(Configuration.VERSION_2_3_21);
            String cfgPathName = mAppCtx.getString(mAppCtx.APP_PROPERTY_CFG_PATH);
            File cfgPathFile = new File(cfgPathName);
            try
            {
                mConfiguration.setDirectoryForTemplateLoading(cfgPathFile);
            }
            catch (IOException e)
            {
                appLogger.error(cfgPathName, e);
            }
            mConfiguration.setObjectWrapper(new DefaultObjectWrapper(Configuration.VERSION_2_3_21));
        }

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
    }

    private String createMessage(DataDoc aDoc)
        throws IOException, FCException
    {
        Logger appLogger = mAppCtx.getLogger(this, "createMessage");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

        initialize();

        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        DataDocConsole dataBagConsole = new DataDocConsole(aDoc);
        dataBagConsole.setUseTitleFlag(true);
        dataBagConsole.writeDoc(printWriter, aDoc.getTitle());
        printWriter.close();

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

        return stringWriter.toString();
    }

    private String createMessage(DataDoc aDoc, String aTemplateFileName)
        throws IOException, FCException
    {
        Logger appLogger = mAppCtx.getLogger(this, "createMessage");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

        initialize();

        Map<String, Object> dataModel = new HashMap<String, Object>();
        for (DataItem dataItem : aDoc.getItems())
            dataModel.put(dataItem.getName(), dataItem.getValue());

        StringWriter stringWriter = new StringWriter();
        try
        {
            Template fmTemplate = mConfiguration.getTemplate(aTemplateFileName);
            fmTemplate.process(dataModel, stringWriter);
        }
        catch (Exception e)
        {
            String msgStr = String.format("%s: %s", aTemplateFileName, e.getMessage());
            appLogger.error(msgStr, e);
            throw new FCException(msgStr);
        }

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

        return stringWriter.toString();
    }

    /**
     * Returns the number of messages previously stored in the internally
     * managed table.
     *
     * @return Count of messages.
     */
    public int messageCount()
    {
        return mDataGrid.rowCount();
    }

    /**
     * Empties the internally managed table of any messages.
     */
    public void reset()
    {
        synchronized(this)
        {
            mDataGrid.emptyRows();
        }
    }

    /**
     * Adds the items contained within the data document to the internally
     * managed data grid.  Refer to <code>schemaMailDocument</code> and
     * <code>schemaServiceMailBag</code> methods.
     *
     * @param aDoc Data bag instance.
     *
     * @return <i>true</i> if the message is valid and added.  <i>false</i>
     * otherwise.
     */
    public boolean addMessage(DataDoc aDoc)
    {
        boolean isValid;
        Logger appLogger = mAppCtx.getLogger(this, "addMessage");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

        if ((aDoc == null) || (! aDoc.isValid()))
            isValid = false;
        else
        {
            synchronized(this)
            {
                mDataGrid.addRow(aDoc);
            }
            isValid = true;
        }

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

        return isValid;
    }

    /**
     * Adds the items contained within the data document to the internally
     * managed table.  Refer to <code>schemaServiceMailBag</code>
     * method.
     *
     * @param anOperation Application defined operation string.
     * @param aStatus Status message of operation.
     * @param aDescription Description of the operation.
     * @param aDetail Details around the operation.
     *
     * @return <i>true</i> if the message is valid and added.  <i>false</i>
     * otherwise.
     */
    public boolean addMessage(String anOperation, String aStatus,
							  String aDescription, String aDetail)
    {
        Logger appLogger = mAppCtx.getLogger(this, "addMessage");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

        DataDoc dataDoc = schemaServiceMailBag();
        dataDoc.setValueByName("msg_operation", anOperation);
        dataDoc.setValueByName("msg_status", aStatus);
        dataDoc.setValueByName("msg_description", aDescription);
        dataDoc.setValueByName("msg_detail", aDetail);

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

        return addMessage(dataDoc);
    }

    /**
     * If the property "delivery_enabled" is <i>true</i>, then this
     * method will deliver the subject and message via an email
     * transport (e.g. SMTP).
     *
     * @param aSubject Message subject.
     * @param aMessage Message content.
     *
     * @throws IOException I/O related error condition.
     * @throws FCException Missing configuration properties.
     * @throws MessagingException Message subsystem error condition.
     */
    public void sendMessage(String aSubject, String aMessage)
        throws IOException, FCException, MessagingException
    {
        InternetAddress internetAddressFrom, internetAddressTo;
        Logger appLogger = mAppCtx.getLogger(this, "sendMessage");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

        if (isCfgStringTrue("delivery_enabled"))
        {
            if ((StringUtils.isNotEmpty(aSubject)) && (StringUtils.isNotEmpty(aMessage)))
            {
                initialize();

                String propertyName = "address_to";
                Message mimeMessage = new MimeMessage(mMailSession);
                if (mAppCtx.isPropertyMultiValue(getCfgString(propertyName)))
                {
                    String[] addressToArray = mAppCtx.getStringArray(getCfgString(propertyName));
                    for (String mailAddressTo : addressToArray)
                    {
                        internetAddressTo = new InternetAddress(mailAddressTo);
                        mimeMessage.addRecipient(MimeMessage.RecipientType.TO, internetAddressTo);
                    }
                }
                else
                {
                    String mailAddressTo = getCfgString(propertyName);
                    if (StringUtils.isEmpty(mailAddressTo))
                    {
                        String msgStr = String.format("Mail Manager property '%s' is undefined.", mCfgMgr.getPrefix() + "." + propertyName);
                        appLogger.error(msgStr);
                        throw new FCException(msgStr);
                    }
                    internetAddressTo = new InternetAddress(mailAddressTo);
                    mimeMessage.addRecipient(MimeMessage.RecipientType.TO, internetAddressTo);
                }
                propertyName = "address_from";
                String mailAddressFrom = getCfgString(propertyName);
                if (StringUtils.isEmpty(mailAddressFrom))
                {
                    String msgStr = String.format("Mail Manager property '%s' is undefined.", mCfgMgr.getPrefix() + "." + propertyName);
                    appLogger.error(msgStr);
                    throw new FCException(msgStr);
                }
                internetAddressFrom = new InternetAddress(mailAddressFrom);
                mimeMessage.addFrom(new InternetAddress[]{internetAddressFrom});
                mimeMessage.setSubject(aSubject);
                mimeMessage.setContent(aMessage, "text/plain");

                appLogger.debug(String.format("Mail Message (%s): %s", aSubject, aMessage));

                Transport.send(mimeMessage);
            }
            else
                throw new FCException("Subject and message are required parameters.");
        }
        else
            appLogger.warn("Email delivery is not enabled - no message will be sent.");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
    }

    /**
     * If the property "delivery_enabled" is <i>true</i>, then this
     * method will deliver the subject and messages stored in the
     * internal table via an email transport (e.g. SMTP).
     *
     * @param aSubject Message subject.
     *
     * @throws IOException I/O related error condition.
     * @throws FCException Missing configuration properties.
     * @throws MessagingException Message subsystem error condition.
     */
    public void sendMessageTable(String aSubject)
        throws IOException, FCException, MessagingException
    {
        Logger appLogger = mAppCtx.getLogger(this, "sendMessageTable");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

        int rowCount = mDataGrid.rowCount();
        if (rowCount > 0)
        {
            String propertyName = "template_service_file";
            String mailTemplatePathFileName = getCfgString(propertyName);
            if (StringUtils.isEmpty(mailTemplatePathFileName))
            {
                String msgStr = String.format("Mail Manager property '%s' is undefined.", mCfgMgr.getPrefix() + "." + propertyName);
                appLogger.error(msgStr);
                throw new FCException(msgStr);
            }

            DataDoc dataDoc = new DataDoc("Mail Message Bag");
            dataDoc.add(new DataItem.Builder().name("msg_table").title("Message Table").build());

            synchronized(this)
            {
                StringWriter stringWriter = new StringWriter();
                PrintWriter printWriter = new PrintWriter(stringWriter);
                DataGridConsole dataTableConsole = new DataGridConsole();
                dataTableConsole.write(mDataGrid, printWriter, StringUtils.EMPTY);
                printWriter.close();
                dataDoc.setValueByName("msg_table", stringWriter.toString());
            }

            String messageBody = createMessage(dataDoc, mailTemplatePathFileName);
            sendMessage(aSubject, messageBody);
        }
        else
            throw new FCException("The message table is empty - nothing to send.");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
    }

    /**
     * If the property "delivery_enabled" is <i>true</i>, then this
     * method will deliver the subject and data document via an email
     * transport (e.g. SMTP).
     *
     * @param aSubject Message subject.
     * @param aDoc Data document instance of items.
     *
     * @throws IOException I/O related error condition.
     * @throws FCException Missing configuration properties.
     * @throws MessagingException Message subsystem error condition.
     */
    public void sendMessageBag(String aSubject, DataDoc aDoc)
        throws IOException, FCException, MessagingException
    {
        Logger appLogger = mAppCtx.getLogger(this, "sendMessageBag");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

        String propertyName = "template_service_file";
        String mailTemplatePathFileName = getCfgString(propertyName);
        if (StringUtils.isEmpty(mailTemplatePathFileName))
        {
            String msgStr = String.format("Mail Manager property '%s' is undefined.", mCfgMgr.getPrefix() + "." + propertyName);
            appLogger.error(msgStr);
            throw new FCException(msgStr);
        }

        String messageBody = createMessage(aDoc, mailTemplatePathFileName);
        sendMessage(aSubject, messageBody);

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
    }

    /**
     * If the property "delivery_enabled" is <i>true</i>, then this method
     * will generate an email message that includes subject, message and
     * attachments to the recipient list.  You can use the convenience
     * methods <i>lookupFromAddress()</i>, <i>createRecipientList()</i>
     * and <i>createAttachmentList()</i> for parameter building assistance.
     *
     * @param aFromAddress Source email address.
     * @param aRecipientList List of recipient email addresses.
     * @param aSubject Subject of the email message.
     * @param aMessage Messsage.
     * @param anAttachmentFiles List of file attachments or <i>null</i> for none.
     *
     * @see <a href="https://www.tutorialspoint.com/javamail_api/javamail_api_send_email_with_attachment.htm">JavaMail API Attachments</a>
     * @see <a href="https://stackoverflow.com/questions/6756162/how-do-i-send-mail-with-both-plain-text-as-well-as-html-text-so-that-each-mail-r">JavaMail API MIME Types</a>
     *
     * @throws IOException I/O related error condition.
     * @throws FCException Missing configuration properties.
     * @throws MessagingException Message subsystem error condition.
     */
    public void sendMessage(String aFromAddress, ArrayList<String> aRecipientList, String aSubject,
                            String aMessage, ArrayList<String> anAttachmentFiles)

        throws IOException, FCException, MessagingException
    {
        InternetAddress internetAddressFrom, internetAddressTo;
        Logger appLogger = mAppCtx.getLogger(this, "sendMessage");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

        if (isCfgStringTrue("delivery_enabled"))
        {
            if ((StringUtils.isNotEmpty(aFromAddress)) && (aRecipientList.size() > 0) &&
                (StringUtils.isNotEmpty(aSubject)) && (StringUtils.isNotEmpty(aMessage)))
            {
                initialize();

                Message mimeMessage = new MimeMessage(mMailSession);
                internetAddressFrom = new InternetAddress(aFromAddress);
                mimeMessage.addFrom(new InternetAddress[]{internetAddressFrom});
                for (String mailAddressTo : aRecipientList)
                {
                    internetAddressTo = new InternetAddress(mailAddressTo);
                    mimeMessage.addRecipient(MimeMessage.RecipientType.TO, internetAddressTo);
                }
                mimeMessage.setSubject(aSubject);

// The following logic create a multi-part message and adds the attachment to it.

                BodyPart messageBodyPart = new MimeBodyPart();
                messageBodyPart.setText(aMessage);
//                messageBodyPart.setContent(aMessage, "text/html");
                Multipart multipart = new MimeMultipart();
                multipart.addBodyPart(messageBodyPart);

                if ((anAttachmentFiles != null) && (anAttachmentFiles.size() > 0))
                {
                    for (String pathFileName : anAttachmentFiles)
                    {
                        File attachmentFile = new File(pathFileName);
                        if (attachmentFile.exists())
                        {
                            messageBodyPart = new MimeBodyPart();
                            DataSource fileDataSource = new FileDataSource(pathFileName);
                            messageBodyPart.setDataHandler(new DataHandler(fileDataSource));
                            messageBodyPart.setFileName(attachmentFile.getName());
                            multipart.addBodyPart(messageBodyPart);
                        }
                    }
                    appLogger.debug(String.format("Mail Message (%s): %s - with attachments", aSubject, aMessage));
                }
                else
                    appLogger.debug(String.format("Mail Message (%s): %s", aSubject, aMessage));

                mimeMessage.setContent(multipart);
                Transport.send(mimeMessage);
            }
            else
                throw new FCException("Valid from, recipient, subject and message are required parameters.");
        }
        else
            appLogger.warn("Email delivery is not enabled - no message will be sent.");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
    }
}
