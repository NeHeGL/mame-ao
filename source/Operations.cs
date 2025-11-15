using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Xml.Linq;

namespace Spludlow.MameAO
{
	public class Operations
	{
		public static int ProcessOperation(Dictionary<string, string> parameters)
		{
			int exitCode;
			ICore core;

			DateTime timeStart = DateTime.Now;

			switch (parameters["operation"])
			{
				//
				//	MAME
				//
				case "mame-get":
					core = new CoreMame();
					core.Initialize(parameters["directory"], parameters["version"]);
					exitCode = core.Get();
					break;

				case "mame-xml":
					core = new CoreMame();
					core.Initialize(parameters["directory"], parameters["version"]);
					core.Xml();
					exitCode = 0;
					break;

				case "mame-json":
					core = new CoreMame();
					core.Initialize(parameters["directory"], parameters["version"]);
					core.Json();
					exitCode = 0;
					break;

				case "mame-sqlite":
					core = new CoreMame();
					core.Initialize(parameters["directory"], parameters["version"]);
					core.SQLite();
					exitCode = 0;
					break;

				case "mame-mssql":
					ValidateRequiredParameters(parameters, new string[] { "server", "names" });
					exitCode = MameMSSQL(parameters["directory"], parameters["version"], parameters["server"], parameters["names"]);
					break;

				case "mame-mssql-payload":
					ValidateRequiredParameters(parameters, new string[] { "server", "names" });
					exitCode = OperationsPayload.MameMSSQLPayloads(parameters["directory"], parameters["version"], parameters["server"], parameters["names"]);
					break;

				//
				// HBMAME
				//
				case "hbmame-get":
					core = new CoreHbMame();
					core.Initialize(parameters["directory"], parameters["version"]);
					exitCode = core.Get();
					break;

				case "hbmame-xml":
					core = new CoreHbMame();
					core.Initialize(parameters["directory"], parameters["version"]);
					core.Xml();
					exitCode = 0;
					break;

				case "hbmame-json":
					core = new CoreHbMame();
					core.Initialize(parameters["directory"], parameters["version"]);
					core.Json();
					exitCode = 0;
					break;

				case "hbmame-sqlite":
					core = new CoreHbMame();
					core.Initialize(parameters["directory"], parameters["version"]);
					core.SQLite();
					exitCode = 0;
					break;

				case "hbmame-mssql":
					ValidateRequiredParameters(parameters, new string[] { "server", "names" });
					exitCode = HbMameMSSQL(parameters["directory"], parameters["version"], parameters["server"], parameters["names"]);
					break;

				case "hbmame-mssql-payload":
					ValidateRequiredParameters(parameters, new string[] { "server", "names" });
					exitCode = OperationsPayload.HbMameMSSQLPayloads(parameters["directory"], parameters["version"], parameters["server"], parameters["names"]);
					break;

				//
				// FBNeo
				//
				case "fbneo-get":
					core = new CoreFbNeo();
					core.Initialize(parameters["directory"], parameters["version"]);
					exitCode = core.Get();
					break;

				case "fbneo-xml":
					core = new CoreFbNeo();
					core.Initialize(parameters["directory"], parameters["version"]);
					core.Xml();
					exitCode = 0;
					break;

				case "fbneo-json":
					core = new CoreFbNeo();
					core.Initialize(parameters["directory"], parameters["version"]);
					core.Json();
					exitCode = 0;
					break;

				case "fbneo-sqlite":
					core = new CoreFbNeo();
					core.Initialize(parameters["directory"], parameters["version"]);
					core.SQLite();
					exitCode = 0;
					break;

				case "fbneo-mssql":
					ValidateRequiredParameters(parameters, new string[] { "server", "names" });
					exitCode = FBNeoMSSQL(parameters["directory"], parameters["version"], parameters["server"], parameters["names"]);
					break;

				case "fbneo-mssql-payload":
					ValidateRequiredParameters(parameters, new string[] { "server", "names" });
					exitCode = OperationsPayload.FBNeoMSSQLPayloads(parameters["directory"], parameters["version"], parameters["server"], parameters["names"]);
					break;

				//
				// TOSEC
				//
				case "tosec-get":
					core = new CoreTosec();
					core.Initialize(parameters["directory"], parameters["version"]);
					exitCode = core.Get();
					break;

				case "tosec-xml":
					core = new CoreTosec();
					core.Initialize(parameters["directory"], parameters["version"]);
					core.Xml();
					exitCode = 0;
					break;

				case "tosec-json":
					core = new CoreTosec();
					core.Initialize(parameters["directory"], parameters["version"]);
					core.Json();
					exitCode = 0;
					break;

				case "tosec-sqlite":
					core = new CoreTosec();
					core.Initialize(parameters["directory"], parameters["version"]);
					core.SQLite();
					exitCode = 0;
					break;

				case "tosec-mssql":
					ValidateRequiredParameters(parameters, new string[] { "server", "names" });
					exitCode = TosecMSSQL(parameters["directory"], parameters["version"], parameters["server"], parameters["names"]);
					break;

				case "tosec-mssql-payload":
					ValidateRequiredParameters(parameters, new string[] { "server", "names" });
					exitCode = OperationsPayload.TosecMSSQLPayloads(parameters["directory"], parameters["version"], parameters["server"], parameters["names"]);
					break;

				default:
					throw new ApplicationException($"Unknown Operation {parameters["operation"]}");
			}

			TimeSpan timeTook = DateTime.Now - timeStart;

			Console.WriteLine($"Operation '{parameters["operation"]}' took: {Math.Round(timeTook.TotalSeconds, 0)} seconds");

			return exitCode;
		}

		private static void ValidateRequiredParameters(Dictionary<string, string> parameters, string[] required)
		{
			List<string> missing = new List<string>();

			foreach (string name in required)
				if (parameters.ContainsKey(name) == false)
					missing.Add(name);

			if (missing.Count > 0)
				throw new ApplicationException($"This operation requires these parameters '{String.Join(", ", missing)}'.");
		}

		//
		// MS SQL
		//
		public static int MameMSSQL(string directory, string version, string serverConnectionString, string databaseNames)
		{
			if (version == "0")
				version = CoreMame.LatestLocalVersion(directory);

			directory = Path.Combine(directory, version);

			string[] xmlFilenames = new string[] {
				Path.Combine(directory, "_machine.xml"),
				Path.Combine(directory, "_software.xml"),
			};

			string[] databaseNamesEach = databaseNames.Split(new char[] { ',' });

			if (databaseNamesEach.Length != 2)
				throw new ApplicationException("database names must be 2 parts comma delimited");

			for (int index = 0; index < 2; ++index)
			{
				string sourceXmlFilename = xmlFilenames[index];
				string targetDatabaseName = databaseNamesEach[index].Trim();

				XElement document = XElement.Load(sourceXmlFilename);
				DataSet dataSet = new DataSet();
				ReadXML.ImportXMLWork(document, dataSet, null, null);

				Database.DataSet2MSSQL(dataSet, serverConnectionString, targetDatabaseName);

				Database.MakeForeignKeys(serverConnectionString, targetDatabaseName);
			}

			return 0;
		}

		public static int HbMameMSSQL(string directory, string version, string serverConnectionString, string databaseNames)
		{
			if (version == "0")
				version = CoreHbMame.LatestLocalVersion(directory);

			directory = Path.Combine(directory, version);

			string[] xmlFilenames = new string[] {
				Path.Combine(directory, "_machine.xml"),
				Path.Combine(directory, "_software.xml"),
			};

			string[] databaseNamesEach = databaseNames.Split(new char[] { ',' });

			if (databaseNamesEach.Length != 2)
				throw new ApplicationException("database names must be 2 parts comma delimited");

			for (int index = 0; index < 2; ++index)
			{
				string sourceXmlFilename = xmlFilenames[index];
				string targetDatabaseName = databaseNamesEach[index].Trim();

				XElement document = XElement.Load(sourceXmlFilename);
				DataSet dataSet = new DataSet();
				ReadXML.ImportXMLWork(document, dataSet, null, null);

				Database.DataSet2MSSQL(dataSet, serverConnectionString, targetDatabaseName);

				Database.MakeForeignKeys(serverConnectionString, targetDatabaseName);
			}

			return 0;
		}

		public static int FBNeoMSSQL(string directory, string version, string serverConnectionString, string databaseName)
		{
			if (version == "0")
				version = CoreFbNeo.FBNeoGetLatestDownloadedVersion(directory);

			directory = Path.Combine(directory, version);

			DataSet dataSet = CoreFbNeo.FBNeoDataSet(directory);

			Database.DataSet2MSSQL(dataSet, serverConnectionString, databaseName);

			Database.MakeForeignKeys(serverConnectionString, databaseName);

			return 0;
		}

		public static int TosecMSSQL(string directory, string version, string serverConnectionString, string databaseName)
		{
			if (version == "0")
				version = CoreTosec.TosecGetLatestDownloadedVersion(directory);

			directory = Path.Combine(directory, version);

			DataSet dataSet = CoreTosec.TosecDataSet(directory);

			Database.DataSet2MSSQL(dataSet, serverConnectionString, databaseName);

			Database.MakeForeignKeys(serverConnectionString, databaseName);

			return 0;
		}

		/// <summary>
		/// Downloads all missing assets of the specified type and places them (including artwork/samples).
		/// This is like .fetch but also places the files and handles artwork/samples.
		/// </summary>
		public static void UpdateAssets(string assetType)
		{
			Tools.ConsoleHeading(1, $"Updating {assetType} Assets");
			Console.WriteLine("This will download all missing files and place them with artwork/samples.");
			Console.WriteLine();

			Globals.WorkerTaskReport = Reports.PlaceReportTemplate();

			switch (assetType.ToUpper())
			{
				case "MR":
					UpdateMachineRoms();
					break;
				case "MD":
					UpdateMachineDisks();
					break;
				case "SR":
					UpdateSoftwareRoms();
					break;
				case "SD":
					UpdateSoftwareDisks();
					break;
				default:
					throw new ApplicationException($"Unknown asset type: {assetType}");
			}

			if (Globals.Settings.Options["PlaceReport"] == "Yes")
				Globals.Reports.SaveHtmlReport(Globals.WorkerTaskReport, $"Update Assets {assetType}");

			Console.WriteLine();
			Tools.ConsoleHeading(1, $"Update {assetType} Complete");
		}

		private static void UpdateMachineRoms()
		{
			DataTable machineTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[0], "SELECT machine_id, name, romof, description FROM machine ORDER BY machine.name");
			DataTable romTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[0], "SELECT machine_id, sha1, name, merge FROM rom WHERE sha1 IS NOT NULL");

			int totalMachines = 0;
			int processedMachines = 0;

			for (int pass = 0; pass < 2; ++pass)
			{
				foreach (DataRow machineRow in pass == 0 ? machineTable.Select("romof IS NULL") : machineTable.Select("romof IS NOT NULL"))
				{
					long machine_id = (long)machineRow["machine_id"];
					string machine_name = (string)machineRow["name"];

					int dontHaveCount = 0;

					foreach (DataRow row in romTable.Select("machine_id = " + machine_id))
					{
						string sha1 = (string)row["sha1"];
						if (Globals.RomHashStore.Exists(sha1) == false)
							++dontHaveCount;
					}

					if (dontHaveCount != 0)
					{
						totalMachines++;
						Console.WriteLine($"\u001b[93m[{totalMachines}]\u001b[0m Processing machine: \u001b[96m{machine_name}\u001b[0m (\u001b[91m{dontHaveCount} missing files\u001b[0m)");
						
						// Download and place
						Place.PlaceMachineRoms(Globals.Core, machine_name, true);
						
						// Place artwork and samples
						DataRow machine = Globals.Core.GetMachine(machine_name);
						if (machine != null)
						{
							Globals.Samples.PlaceAssets(Globals.Core.Directory, machine);
							Globals.Artwork.PlaceAssets(Globals.Core.Directory, machine);
						}
						
						processedMachines++;
					}
				}
			}

			Console.WriteLine($"\u001b[92mProcessed {processedMachines} of {totalMachines} machines with missing ROMs.\u001b[0m");
		}

		private static void UpdateMachineDisks()
		{
			DataTable machineTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[0], "SELECT machine_id, name, description FROM machine ORDER BY machine.name");
			DataTable diskTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[0], "SELECT machine_id, sha1, name, merge FROM disk WHERE sha1 IS NOT NULL");

			int totalMachines = 0;
			int processedMachines = 0;

			foreach (DataRow machineRow in machineTable.Rows)
			{
				long machine_id = (long)machineRow["machine_id"];
				string machine_name = (string)machineRow["name"];

				int dontHaveCount = 0;

				foreach (DataRow row in diskTable.Select("machine_id = " + machine_id))
				{
					string sha1 = (string)row["sha1"];
					if (Globals.DiskHashStore.Exists(sha1) == false)
						++dontHaveCount;
				}

				if (dontHaveCount != 0)
				{
					totalMachines++;
					Console.WriteLine($"\u001b[93m[{totalMachines}]\u001b[0m Processing machine: \u001b[96m{machine_name}\u001b[0m (\u001b[91m{dontHaveCount} missing disks\u001b[0m)");
					
					// Download and place
					Place.PlaceMachineDisks(Globals.Core, machine_name, true);
					
					// Place artwork and samples
					DataRow machine = Globals.Core.GetMachine(machine_name);
					if (machine != null)
					{
						Globals.Samples.PlaceAssets(Globals.Core.Directory, machine);
						Globals.Artwork.PlaceAssets(Globals.Core.Directory, machine);
					}
					
					processedMachines++;
				}
			}

			Console.WriteLine($"\u001b[92mProcessed {processedMachines} of {totalMachines} machines with missing disks.\u001b[0m");
		}

		private static void UpdateSoftwareRoms()
		{
			DataTable softwarelistTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[1], "SELECT softwarelist.softwarelist_id, softwarelist.name, softwarelist.description FROM softwarelist ORDER BY softwarelist.name");
			DataTable softwareTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[1], "SELECT software.software_id, software.softwarelist_id, software.name, software.description, software.cloneof FROM software ORDER BY software.name");
			DataTable romTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[1], "SELECT part.software_id, rom.name, rom.sha1 FROM (part INNER JOIN dataarea ON part.part_id = dataarea.part_id) INNER JOIN rom ON dataarea.dataarea_id = rom.dataarea_id WHERE (rom.sha1 IS NOT NULL)");

			int totalSoftware = 0;
			int processedSoftware = 0;

			foreach (DataRow softwarelistRow in softwarelistTable.Rows)
			{
				long softwarelist_id = (long)softwarelistRow["softwarelist_id"];
				string softwarelist_name = (string)softwarelistRow["name"];
				
				foreach (DataRow softwareRow in softwareTable.Select($"softwarelist_id = {softwarelist_id}"))
				{
					int dontHaveCount = 0;

					long software_id = (long)softwareRow["software_id"];
					string software_name = (string)softwareRow["name"];
					
					foreach (DataRow romRow in romTable.Select($"software_id = {software_id}"))
					{
						string sha1 = (string)romRow["sha1"];
						if (Globals.RomHashStore.Exists(sha1) == false)
							++dontHaveCount;
					}

					if (dontHaveCount != 0)
					{
						totalSoftware++;
						Console.WriteLine($"\u001b[93m[{totalSoftware}]\u001b[0m Processing software: \u001b[96m{softwarelist_name}/{software_name}\u001b[0m (\u001b[91m{dontHaveCount} missing files\u001b[0m)");
						
						// Download and place
						Place.PlaceSoftwareRoms(Globals.Core, softwarelistRow, softwareRow, true);
						
						processedSoftware++;
					}
				}
			}

			Console.WriteLine($"\u001b[92mProcessed {processedSoftware} of {totalSoftware} software items with missing ROMs.\u001b[0m");
		}

		private static void UpdateSoftwareDisks()
		{
			List<string> ignoreListNames = new List<string>();
			if (Globals.Config.ContainsKey("SoftwareListSkip") == true)
				ignoreListNames.AddRange(Globals.Config["SoftwareListSkip"].Split(',').Select(item => item.Trim()));

			DataTable softwarelistTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[1], "SELECT softwarelist.softwarelist_id, softwarelist.name, softwarelist.description FROM softwarelist ORDER BY softwarelist.name");
			DataTable softwareTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[1], "SELECT software.software_id, software.softwarelist_id, software.name, software.description, software.cloneof FROM software ORDER BY software.name");
			DataTable diskTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[1], "SELECT part.software_id, disk.name, disk.sha1 FROM (part INNER JOIN diskarea ON part.part_id = diskarea.part_id) INNER JOIN disk ON diskarea.diskarea_id = disk.diskarea_id WHERE (disk.sha1 IS NOT NULL)");

			// First pass: count total software with missing disks and collect them
			List<Tuple<DataRow, DataRow, int>> softwareToProcess = new List<Tuple<DataRow, DataRow, int>>();

			foreach (DataRow softwarelistRow in softwarelistTable.Rows)
			{
				string softwarelist_name = (string)softwarelistRow["name"];

				if (ignoreListNames.Contains(softwarelist_name) == true)
					continue;

				long softwarelist_id = (long)softwarelistRow["softwarelist_id"];
				
				foreach (DataRow softwareRow in softwareTable.Select($"softwarelist_id = {softwarelist_id}"))
				{
					int dontHaveCount = 0;
					long software_id = (long)softwareRow["software_id"];
					
					foreach (DataRow diskRow in diskTable.Select($"software_id = {software_id}"))
					{
						string sha1 = (string)diskRow["sha1"];
						if (Globals.DiskHashStore.Exists(sha1) == false)
							++dontHaveCount;
					}

					if (dontHaveCount > 0)
					{
						softwareToProcess.Add(new Tuple<DataRow, DataRow, int>(softwarelistRow, softwareRow, dontHaveCount));
					}
				}
			}

			int totalSoftware = softwareToProcess.Count;
			Console.WriteLine($"Found \u001b[96m{totalSoftware}\u001b[0m software items with missing disk files.");
			Console.WriteLine();

			// Second pass: process software
			int currentSoftware = 0;
			foreach (var item in softwareToProcess)
			{
				currentSoftware++;
				DataRow softwarelistRow = item.Item1;
				DataRow softwareRow = item.Item2;
				int dontHaveCount = item.Item3;

				string softwarelist_name = (string)softwarelistRow["name"];
				string software_name = (string)softwareRow["name"];

				Console.WriteLine($"\u001b[93m[{currentSoftware}/{totalSoftware}]\u001b[0m Processing software: \u001b[96m{softwarelist_name}/{software_name}\u001b[0m (\u001b[91m{dontHaveCount} missing disks\u001b[0m)");
				
				// Download and place
				Place.PlaceSoftwareDisks(Globals.Core, softwarelistRow, softwareRow, true);
			}

			Console.WriteLine($"\u001b[92mProcessed {currentSoftware} of {totalSoftware} software items with missing disks.\u001b[0m");
		}

		/// <summary>
		/// Places software ROM and/or disk assets for a specific machine and software combination.
		/// </summary>
		public static void PlaceSoftwareAssets(ICore core, string machineName, string softwareName, bool placeRoms, bool placeDisks)
		{
			DataRow machine = core.GetMachine(machineName);
			if (machine == null)
				throw new ApplicationException($"Machine not found: {machineName}");

			DataRow[] softwarelists = core.GetMachineSoftwareLists(machine);
			bool softwareFound = false;

			foreach (DataRow machineSoftwarelist in softwarelists)
			{
				string softwarelistName = (string)machineSoftwarelist["name"];
				DataRow softwarelist = core.GetSoftwareList(softwarelistName);

				if (softwarelist == null)
				{
					Console.WriteLine($"!!! DATA Error Machine's '{machineName}' software list '{softwarelistName}' missing.");
					continue;
				}

				foreach (DataRow findSoftware in core.GetSoftwareListsSoftware(softwarelist))
				{
					if ((string)findSoftware["name"] == softwareName)
					{
						if (placeRoms)
							Place.PlaceSoftwareRoms(core, softwarelist, findSoftware, true);
						
						if (placeDisks)
							Place.PlaceSoftwareDisks(core, softwarelist, findSoftware, true);

						softwareFound = true;
						break;
					}
				}

				if (softwareFound)
					break;
			}

			if (!softwareFound)
				throw new ApplicationException($"Software not found: {machineName}, {softwareName}");
		}

	}
}
