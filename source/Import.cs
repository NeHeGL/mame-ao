﻿using System;
using System.Collections.Generic;
using System.Data;
using System.IO.Compression;
using System.IO;
using System.Linq;

namespace Spludlow.MameAO
{
	public class Import
	{
		public static void ImportDirectory(string importDirectory)
		{
			if (Directory.Exists(importDirectory) == false)
				throw new ApplicationException($"Import directory does not exist: {importDirectory}");

			Tools.ConsoleHeading(1, new string[] {
				"Import from Directory",
				importDirectory,
			});

			DataTable reportTable = Tools.MakeDataTable(
				"Filename	Type	SHA1	Action",
				"String		String	String	String"
			);

			ImportDirectory(importDirectory, Globals.AllSHA1, reportTable);

			Globals.Reports.SaveHtmlReport(reportTable, "Import Directory");
		}
		public static void ImportDirectory(string importDirectory, HashSet<string> allSHA1s, DataTable reportTable)
		{
			foreach (string filename in Directory.GetFiles(importDirectory, "*", SearchOption.AllDirectories))
			{
				Console.WriteLine(filename);

				string name = filename.Substring(importDirectory.Length + 1);

				string extention = Path.GetExtension(filename).ToLower();

				string sha1;
				string status;

				switch (extention)
				{
					case ".zip":
						sha1 = "";
						status = "";

						using (TempDirectory tempDir = new TempDirectory())
						{
							ZipFile.ExtractToDirectory(filename, tempDir.Path);

							Tools.ClearAttributes(tempDir.Path);

							reportTable.Rows.Add(name, "ARCHIVE", sha1, status);

							ImportDirectory(tempDir.Path, allSHA1s, reportTable);
						}
						break;

					case ".chd":
						sha1 = Globals.DiskHashStore.Hash(filename);
						if (allSHA1s.Contains(sha1) == true)
							status = Globals.DiskHashStore.Add(filename, false, sha1) ? "" : "Have";
						else
							status = "Unknown";

						reportTable.Rows.Add(name, "DISK", sha1, status);
						break;

					default:
						sha1 = Globals.RomHashStore.Hash(filename);
						if (allSHA1s.Contains(sha1) == true)
							status = Globals.RomHashStore.Add(filename, false, sha1) ? "" : "Have";
						else
							status = "Unknown";

						reportTable.Rows.Add(name, "ROM", sha1, status);
						break;
				}
			}
		}

		public static void FetchMachineRom()
		{
			DataTable machineTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[0], "SELECT machine_id, name, romof, description FROM machine ORDER BY machine.name");
			DataTable romTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[0], "SELECT machine_id, sha1, name, merge FROM rom WHERE sha1 IS NOT NULL");

			Globals.WorkerTaskReport = Reports.PlaceReportTemplate();

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
						Place.PlaceMachineRoms(Globals.Core, machine_name, false);
				}
			}
		}
		public static void FetchMachineDisk()
		{
			DataTable machineTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[0], "SELECT machine_id, name, description FROM machine ORDER BY machine.name");
			DataTable diskTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[0], "SELECT machine_id, sha1, name, merge FROM disk WHERE sha1 IS NOT NULL");

			Globals.WorkerTaskReport = Reports.PlaceReportTemplate();

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
					Place.PlaceMachineDisks(Globals.Core, machine_name, false);
			}
		}

		public static void FetchSoftwareRom()
		{
			DataTable softwarelistTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[1], "SELECT softwarelist.softwarelist_id, softwarelist.name, softwarelist.description FROM softwarelist ORDER BY softwarelist.name");
			DataTable softwareTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[1], "SELECT software.software_id, software.softwarelist_id, software.name, software.description, software.cloneof FROM software ORDER BY software.name");
			DataTable romTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[1], "SELECT part.software_id, rom.name, rom.sha1 FROM (part INNER JOIN dataarea ON part.part_id = dataarea.part_id) INNER JOIN rom ON dataarea.dataarea_id = rom.dataarea_id WHERE (rom.sha1 IS NOT NULL)");

			Globals.WorkerTaskReport = Reports.PlaceReportTemplate();

			foreach (DataRow softwarelistRow in softwarelistTable.Rows)
			{
				long softwarelist_id = (long)softwarelistRow["softwarelist_id"];
				foreach (DataRow softwareRow in softwareTable.Select($"softwarelist_id = {softwarelist_id}"))
				{
					int dontHaveCount = 0;

					long software_id = (long)softwareRow["software_id"];
					foreach (DataRow romRow in romTable.Select($"software_id = {software_id}"))
					{
						string sha1 = (string)romRow["sha1"];
						if (Globals.RomHashStore.Exists(sha1) == false)
							++dontHaveCount;
					}

					if (dontHaveCount != 0)
						Place.PlaceSoftwareRoms(Globals.Core, softwarelistRow, softwareRow, false);
				}
			}
		}

		public static void FetchSoftwareDisk()
		{
			List<string> ignoreListNames = new List<string>();
			if (Globals.Config.ContainsKey("SoftwareListSkip") == true)
				ignoreListNames.AddRange(Globals.Config["SoftwareListSkip"].Split(',').Select(item => item.Trim()));

			DataTable softwarelistTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[1], "SELECT softwarelist.softwarelist_id, softwarelist.name, softwarelist.description FROM softwarelist ORDER BY softwarelist.name");
			DataTable softwareTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[1], "SELECT software.software_id, software.softwarelist_id, software.name, software.description, software.cloneof FROM software ORDER BY software.name");
			DataTable diskTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[1], "SELECT part.software_id, disk.name, disk.sha1 FROM (part INNER JOIN diskarea ON part.part_id = diskarea.part_id) INNER JOIN disk ON diskarea.diskarea_id = disk.diskarea_id WHERE (disk.sha1 IS NOT NULL)");

			Globals.WorkerTaskReport = Reports.PlaceReportTemplate();

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
						Place.PlaceSoftwareDisks(Globals.Core, softwarelistRow, softwareRow, false);
				}
			}
		}

		public static DataSet PlaceSoftwareList(string softwareListName, bool placeFiles)
		{
			DataTable softwarelistTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[1],
				$"SELECT softwarelist.softwarelist_id, softwarelist.name, softwarelist.description FROM softwarelist WHERE (softwarelist.name = '{softwareListName}')");

			if (softwarelistTable.Rows.Count != 1)
				throw new ApplicationException($"Software List not found: {softwareListName}");

			DataTable softwareTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[1], "SELECT software.software_id, software.softwarelist_id, software.name, software.description, software.cloneof FROM software ORDER BY software.name");
			DataTable romTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[1], "SELECT part.software_id, rom.name, rom.sha1 FROM (part INNER JOIN dataarea ON part.part_id = dataarea.part_id) INNER JOIN rom ON dataarea.dataarea_id = rom.dataarea_id WHERE (rom.sha1 IS NOT NULL)");
			DataTable diskTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[1], "SELECT part.software_id, disk.name, disk.sha1 FROM (part INNER JOIN diskarea ON part.part_id = diskarea.part_id) INNER JOIN disk ON diskarea.diskarea_id = disk.diskarea_id WHERE (disk.sha1 IS NOT NULL)");

			Globals.WorkerTaskReport = Reports.PlaceReportTemplate();

			foreach (DataRow softwarelistRow in softwarelistTable.Rows)
			{
				long softwarelist_id = (long)softwarelistRow["softwarelist_id"];

				foreach (DataRow softwareRow in softwareTable.Select($"softwarelist_id = {softwarelist_id}"))
				{
					Place.PlaceSoftwareRoms(Globals.Core, softwarelistRow, softwareRow, placeFiles);
					Place.PlaceSoftwareDisks(Globals.Core, softwarelistRow, softwareRow, placeFiles);
				}
			}

			if (Globals.Settings.Options["PlaceReport"] == "Yes")
				Globals.Reports.SaveHtmlReport(Globals.WorkerTaskReport, $"Place Assets Software List - {softwareListName}");

			DataSet dataSet = new DataSet();

			DataTable[] tables = new DataTable[] { softwarelistTable, softwareTable, romTable, diskTable };
			string[] names = new string[] { "softwarelist", "software", "rom", "disk" };
			for (int index = 0; index < tables.Length; ++index)
			{
				DataTable table = tables[index];
				table.TableName = names[index];
				dataSet.Tables.Add(table);
			}

			return dataSet;
		}

	}
}
