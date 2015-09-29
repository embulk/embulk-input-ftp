package org.embulk.input.integration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.FtpFileInputPlugin;
import org.embulk.spi.FileInputRunner;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.util.Pages;
import org.embulk.standards.CsvParserPlugin;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

public class TestFtpFileInputPlugin
{
    private static String FTP_HOST;
    private static String FTP_USER;
    private static String FTP_PASSWORD;
    private static String FTP_SSL_TRUSTED_CA_CERT_FILE;
    private static String FTP_SSL_TRUSTED_CA_CERT_DATA;
    private static final String FTP_PATH_PREFIX = "/embulk_input_ftp_test";

    /*
     * This test case requires environment variables:
     *   FTP_TEST_HOST
     *   FTP_TEST_USER
     *   FTP_TEST_PASSWORD
     *   FTP_TEST_SSSL_TRUSTED_CA_CERT_FILE
     *   FTP_TEST_SSSL_TRUSTED_CA_CERT_DATA
     * If the variables not set, the test case is skipped.
     */
    @BeforeClass
    public static void initializeConstantVariables()
    {
        FTP_HOST = System.getenv("FTP_TEST_HOST");
        FTP_USER = System.getenv("FTP_TEST_USER");
        FTP_PASSWORD = System.getenv("FTP_TEST_PASSWORD");
        FTP_SSL_TRUSTED_CA_CERT_FILE = System.getenv("FTP_TEST_SSL_TRUSTED_CA_CERT_FILE");
        FTP_SSL_TRUSTED_CA_CERT_DATA = System.getenv("FTP_TEST_SSL_TRUSTED_CA_CERT_DATA");
        assumeNotNull(FTP_HOST, FTP_USER, FTP_PASSWORD, FTP_SSL_TRUSTED_CA_CERT_FILE, FTP_SSL_TRUSTED_CA_CERT_DATA);
    }

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ConfigSource config;
    private FileInputRunner runner;
    private MockPageOutput output;

    @Before
    public void createResources()
    {
        config = runtime.getExec().newConfigSource()
                .set("type", "ftp")
                .set("host", FTP_HOST)
                .set("user", FTP_USER)
                .set("password", FTP_PASSWORD)
                .set("path_prefix", FTP_PATH_PREFIX)
                .set("parser", parserConfig(schemaConfig()));
        runner = new FileInputRunner(runtime.getInstance(FtpFileInputPlugin.class));
        output = new MockPageOutput();
    }

    @Test
    public void simpleTest()
    {
        ConfigSource config = this.config.deepCopy();
        ConfigDiff configDiff = runner.transaction(config, new Control());

        assertEquals(FTP_PATH_PREFIX + "/sample_01.csv", configDiff.get(String.class, "last_path"));
        assertRecords(config, output);
    }

    @Test
    public void notIncludeSlashInPathPrefix()
    {
        ConfigSource config = this.config.deepCopy().set("path_prefix", "embulk_input_ftp_test"); // not include '/'
        ConfigDiff configDiff = runner.transaction(config, new Control());

        assertEquals(FTP_PATH_PREFIX + "/sample_01.csv", configDiff.get(String.class, "last_path"));
        assertRecords(config, output);
    }

    @Test
    public void useLastPath()
            throws Exception
    {
        ConfigSource config = this.config.deepCopy().set("last_path", FTP_PATH_PREFIX + "/sample_01.csv");
        ConfigDiff configDiff = runner.transaction(config, new Control());

        assertEquals(FTP_PATH_PREFIX + "/sample_01.csv", configDiff.get(String.class, "last_path"));
        assertEquals(0, getRecords(config, output).size());
    }

    @Test
    public void emptyFilesWithLastPath()
            throws Exception
    {
        ConfigSource config = this.config.deepCopy()
                .set("path_prefix", "empty_files_prefix")
                .set("last_path", FTP_PATH_PREFIX + "/sample_01.csv");
        ConfigDiff configDiff = runner.transaction(config, new Control());

        assertEquals(FTP_PATH_PREFIX + "/sample_01.csv", configDiff.get(String.class, "last_path")); // keep the last_path
        assertEquals(0, getRecords(config, output).size());
    }

    @Test
    public void useAsciiModeTrue()
            throws Exception
    {
        ConfigSource config = this.config.deepCopy().set("ascii_mode", true);
        ConfigDiff configDiff = runner.transaction(config, new Control());

        assertEquals(FTP_PATH_PREFIX + "/sample_01.csv", configDiff.get(String.class, "last_path"));
        assertRecords(config, output);
    }

    @Test
    public void useSslWithoutSslVerify()
            throws Exception
    {
        ConfigSource config = this.config.deepCopy()
                .set("port", 990)
                .set("ssl", true)
                .set("ssl_verify", false);
        ConfigDiff configDiff = runner.transaction(config, new Control());

        assertEquals(FTP_PATH_PREFIX + "/sample_01.csv", configDiff.get(String.class, "last_path"));
        assertRecords(config, output);
    }

    @Test
    public void useSslWithCertFile()
            throws Exception
    {
        ConfigSource config = this.config.deepCopy()
                .set("port", 990)
                .set("ssl", true)
                .set("ssl_verify", true)
                .set("ssl_verify_hostname", "false")
                .set("ssl_trusted_ca_cert_file", FTP_SSL_TRUSTED_CA_CERT_FILE);
        ConfigDiff configDiff = runner.transaction(config, new Control());

        assertEquals(FTP_PATH_PREFIX + "/sample_01.csv", configDiff.get(String.class, "last_path"));
        assertRecords(config, output);
    }

    @Test
    public void useSslWithCertData()
            throws Exception
    {
        ConfigSource config = this.config.deepCopy()
                .set("port", 990)
                .set("ssl", true)
                .set("ssl_verify", true)
                .set("ssl_verify_hostname", "false")
                .set("ssl_trusted_ca_cert_data", FTP_SSL_TRUSTED_CA_CERT_DATA);
        ConfigDiff configDiff = runner.transaction(config, new Control());

        assertEquals(FTP_PATH_PREFIX + "/sample_01.csv", configDiff.get(String.class, "last_path"));
        assertRecords(config, output);
    }

    private class Control
            implements InputPlugin.Control
    {
        @Override
        public List<TaskReport> run(TaskSource taskSource, Schema schema, int taskCount)
        {
            List<TaskReport> reports = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                reports.add(runner.run(taskSource, schema, i, output));
            }
            return reports;
        }
    }

    private ImmutableMap<String, Object> parserConfig(ImmutableList<Object> schemaConfig)
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("newline", "CRLF");
        builder.put("delimiter", ",");
        builder.put("quote", "\"");
        builder.put("escape", "\"");
        builder.put("trim_if_not_quoted", false);
        builder.put("skip_header_lines", 0);
        builder.put("allow_extra_columns", false);
        builder.put("allow_optional_columns", false);
        builder.put("columns", schemaConfig);
        return builder.build();
    }

    private ImmutableList<Object> schemaConfig()
    {
        ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>();
        builder.add(ImmutableMap.of("name", "timestamp", "type", "timestamp", "format", "%Y-%m-%d %H:%M:%S"));
        builder.add(ImmutableMap.of("name", "host", "type", "string"));
        builder.add(ImmutableMap.of("name", "path", "type", "string"));
        builder.add(ImmutableMap.of("name", "method", "type", "string"));
        builder.add(ImmutableMap.of("name", "referer", "type", "string"));
        builder.add(ImmutableMap.of("name", "code", "type", "long"));
        builder.add(ImmutableMap.of("name", "agent", "type", "string"));
        builder.add(ImmutableMap.of("name", "user", "type", "string"));
        builder.add(ImmutableMap.of("name", "size", "type", "long"));
        return builder.build();
    }

    private void assertRecords(ConfigSource config, MockPageOutput output)
    {
        List<Object[]> records = getRecords(config, output);

        assertEquals(2, records.size());
        {
            Object[] record = records.get(0);
            assertEquals("2014-10-02 22:15:39 UTC", record[0].toString());
            assertEquals("84.186.29.187", record[1]);
            assertEquals("/category/electronics", record[2]);
            assertEquals("GET", record[3]);
            assertEquals("/category/music", record[4]);
            assertEquals(200L, record[5]);
            assertEquals("Mozilla/5.0", record[6]);
            assertEquals("-", record[7]);
            assertEquals(136L, record[8]);
        }

        {
            Object[] record = records.get(1);
            assertEquals("2014-10-02 22:15:01 UTC", record[0].toString());
            assertEquals("140.36.216.47", record[1]);
            assertEquals("/category/music?from=10", record[2]);
            assertEquals("GET", record[3]);
            assertEquals("-", record[4]);
            assertEquals(200L, record[5]);
            assertEquals("Mozilla/5.0", record[6]);
            assertEquals("-", record[7]);
            assertEquals(70L, record[8]);
        }
    }

    private List<Object[]> getRecords(ConfigSource config, MockPageOutput output)
    {
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        return Pages.toObjects(schema, output.pages);
    }
}
