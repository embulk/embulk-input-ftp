package org.embulk.input;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.FtpFileInputPlugin.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileInputRunner;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.util.Pages;
import org.embulk.standards.CsvParserPlugin;
import org.embulk.util.ftp.SSLPlugins;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeNotNull;

public class TestFtpFileInputPlugin
{
    private static String FTP_TEST_HOST;
    private static Integer FTP_TEST_PORT;
    private static Integer FTP_TEST_SSL_PORT;
    private static String FTP_TEST_USER;
    private static String FTP_TEST_PASSWORD;
    private static String FTP_TEST_SSL_TRUSTED_CA_CERT_FILE;
    private static String FTP_TEST_SSL_TRUSTED_CA_CERT_DATA;
    private static String FTP_TEST_DIRECTORY;
    private static String FTP_TEST_PATH_PREFIX;
    private FileInputRunner runner;
    private TestPageBuilderReader.MockPageOutput output;

    /*
     * This test case requires environment variables
     *   FTP_TEST_HOST
     *   FTP_TEST_USER
     *   FTP_TEST_PASSWORD
     *   FTP_TEST_SSL_TRUSTED_CA_CERT_FILE
     */
    @BeforeClass
    public static void initializeConstant()
    {
        FTP_TEST_HOST = System.getenv("FTP_TEST_HOST");
        FTP_TEST_PORT = System.getenv("FTP_TEST_PORT") != null ? Integer.valueOf(System.getenv("FTP_TEST_PORT")) : 21;
        FTP_TEST_SSL_PORT = System.getenv("FTP_TEST_SSL_PORT") != null ? Integer.valueOf(System.getenv("FTP_TEST_SSL_PORT")) : 990;
        FTP_TEST_USER = System.getenv("FTP_TEST_USER");
        FTP_TEST_PASSWORD = System.getenv("FTP_TEST_PASSWORD");
        FTP_TEST_SSL_TRUSTED_CA_CERT_FILE = System.getenv("FTP_TEST_SSL_TRUSTED_CA_CERT_FILE");
        FTP_TEST_SSL_TRUSTED_CA_CERT_DATA = System.getenv("FTP_TEST_SSL_TRUSTED_CA_CERT_DATA");
        // skip test cases, if environment variables are not set.
        assumeNotNull(FTP_TEST_HOST, FTP_TEST_USER, FTP_TEST_PASSWORD, FTP_TEST_SSL_TRUSTED_CA_CERT_FILE, FTP_TEST_SSL_TRUSTED_CA_CERT_DATA);

        FTP_TEST_DIRECTORY = System.getenv("FTP_TEST_DIRECTORY") != null ? getDirectory(System.getenv("FTP_TEST_DIRECTORY")) : getDirectory("/unittest/");
        FTP_TEST_PATH_PREFIX = FTP_TEST_DIRECTORY + "sample_";
    }

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private FtpFileInputPlugin plugin;

    @Before
    public void createResources()
    {
        plugin = new FtpFileInputPlugin();
        runner = new FileInputRunner(runtime.getInstance(FtpFileInputPlugin.class));
        output = new MockPageOutput();
    }

    @Test(expected = RuntimeException.class) // TODO ConfigException should be thrown
    public void testTransactionWithInvalidHost()
    {
        ConfigSource config = config().deepCopy()
                .set("host", "non-exists.example.com");

        runner.transaction(config, new Control());
    }

    @Test
    public void testResume()
    {
        PluginTask task = config().loadConfig(PluginTask.class);
        task.setSSLConfig(sslConfig(task));
        task.setFiles(Arrays.asList("in/aa/a"));
        ConfigDiff configDiff = plugin.resume(task.dump(), 0, new FileInputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource, int taskCount)
            {
                return emptyTaskReports(taskCount);
            }
        });
        assertThat(configDiff.get(String.class, "last_path"), is("in/aa/a"));
    }

    @Test
    public void testCleanup()
    {
        PluginTask task = config().loadConfig(PluginTask.class);
        plugin.cleanup(task.dump(), 0, Lists.<TaskReport>newArrayList()); // no errors happens
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testListFilesWithNonExistPath() throws Exception
    {
        ConfigSource config = config().deepCopy()
                .set("path_prefix", "non-exists-path");
        PluginTask task = config.loadConfig(PluginTask.class);
        plugin.transaction(config, new FileInputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource, int taskCount)
            {
                assertThat(taskCount, is(0));
                return emptyTaskReports(taskCount);
            }
        });

        Method method = FtpFileInputPlugin.class.getDeclaredMethod("listFiles", Logger.class, PluginTask.class);
        method.setAccessible(true);
        Logger logger = Exec.getLogger(FtpFileInputPlugin.class);
        List<String> fileList = (List<String>) method.invoke(plugin, logger, task);
        assertThat(fileList.size(), is(0));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testListFiles() throws Exception
    {
        List<String> expected = Arrays.asList(
            FTP_TEST_PATH_PREFIX + "01.csv",
            FTP_TEST_PATH_PREFIX + "02.csv"
        );

        ConfigSource config = config();
        final PluginTask task = config.loadConfig(PluginTask.class);
        ConfigDiff configDiff = plugin.transaction(config, new FileInputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource, int taskCount)
            {
                assertThat(taskCount, is(2));
                return emptyTaskReports(taskCount);
            }
        });

        Method method = FtpFileInputPlugin.class.getDeclaredMethod("listFiles", Logger.class, PluginTask.class);
        method.setAccessible(true);
        Logger logger = Exec.getLogger(FtpFileInputPlugin.class);
        List<String> fileList = (List<String>) method.invoke(plugin, logger, task);
        assertThat(fileList.get(0), is(expected.get(0)));
        assertThat(fileList.get(1), is(expected.get(1)));
        assertThat(configDiff.get(String.class, "last_path"), is(FTP_TEST_PATH_PREFIX + "02.csv"));
    }

    @Test
    public void testListFilesByPrefixIncrementalFalse()
    {
        ConfigSource config = config()
                .deepCopy()
                .set("incremental", false);

        ConfigDiff configDiff = runner.transaction(config, new Control());

        assertThat(configDiff.toString(), is("{}"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGcsFileInputByOpen() throws Exception
    {
        ConfigSource config = config();
        PluginTask task = config().loadConfig(PluginTask.class);

        runner.transaction(config, new Control());

        Method method = FtpFileInputPlugin.class.getDeclaredMethod("listFiles", Logger.class, PluginTask.class);
        method.setAccessible(true);
        Logger logger = Exec.getLogger(FtpFileInputPlugin.class);
        List<String> fileList = (List<String>) method.invoke(plugin, logger, task);
        task.setFiles(fileList);

        assertRecords(config, output);
    }

    private static List<TaskReport> emptyTaskReports(int taskCount)
    {
        ImmutableList.Builder<TaskReport> reports = new ImmutableList.Builder<>();
        for (int i = 0; i < taskCount; i++) {
            reports.add(Exec.newTaskReport());
        }
        return reports.build();
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

    private ConfigSource config()
    {
        return Exec.newConfigSource()
                .set("host", FTP_TEST_HOST)
                .set("port", FTP_TEST_PORT)
                .set("user", FTP_TEST_USER)
                .set("password", FTP_TEST_PASSWORD)
                .set("path_prefix", FTP_TEST_PATH_PREFIX)
                .set("last_path", "")
                .set("file_ext", ".csv")
                .set("max_connection_retry", 3)
                .set("ssl", false)
                .set("ssl_verify", false)
                .set("ssl_verify_hostname", false)
                .set("ssl_trusted_ca_cert_data", FTP_TEST_SSL_TRUSTED_CA_CERT_DATA)
                .set("parser", parserConfig(schemaConfig()));
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
        builder.put("skip_header_lines", 1);
        builder.put("allow_extra_columns", false);
        builder.put("allow_optional_columns", false);
        builder.put("columns", schemaConfig);
        return builder.build();
    }

    private ImmutableList<Object> schemaConfig()
    {
        ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>();
        builder.add(ImmutableMap.of("name", "id", "type", "long"));
        builder.add(ImmutableMap.of("name", "account", "type", "long"));
        builder.add(ImmutableMap.of("name", "time", "type", "timestamp", "format", "%Y-%m-%d %H:%M:%S"));
        builder.add(ImmutableMap.of("name", "purchase", "type", "timestamp", "format", "%Y%m%d"));
        builder.add(ImmutableMap.of("name", "comment", "type", "string"));
        return builder.build();
    }

    public SSLPlugins.SSLPluginConfig sslConfig(PluginTask task)
    {
        return SSLPlugins.configure(task);
    }

    private void assertRecords(ConfigSource config, MockPageOutput output)
    {
        List<Object[]> records = getRecords(config, output);
        assertThat(records.size(), is(8));
        {
            Object[] record = records.get(0);
            assertThat((long) record[0], is(1L));
            assertThat((long) record[1], is(32864L));
            assertThat(record[2].toString(), is("2015-01-27 19:23:49 UTC"));
            assertThat(record[3].toString(), is("2015-01-27 00:00:00 UTC"));
            assertThat(record[4].toString(), is("embulk"));
        }

        {
            Object[] record = records.get(1);
            assertThat((long) record[0], is(2L));
            assertThat((long) record[1], is(14824L));
            assertThat(record[2].toString(), is("2015-01-27 19:01:23 UTC"));
            assertThat(record[3].toString(), is("2015-01-27 00:00:00 UTC"));
            assertThat(record[4].toString(), is("embulk jruby"));
        }
    }

    private List<Object[]> getRecords(ConfigSource config, MockPageOutput output)
    {
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        return Pages.toObjects(schema, output.pages);
    }

    private static String getDirectory(String dir)
    {
        if (dir != null && !dir.endsWith("/")) {
            dir = dir + "/";
        }
        if (dir.startsWith("/")) {
            dir = dir.replaceFirst("/", "");
        }
        return dir;
    }
}
