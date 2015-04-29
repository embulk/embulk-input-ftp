Embulk::JavaPlugin.register_input(
  "ftp", "org.embulk.input.FtpFileInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
