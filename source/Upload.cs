﻿using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.IO.Compression;
using System.Net;
using System.Threading;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Spludlow.MameAO
{
	public class Upload
	{
		public static readonly DateTime MagicModifiedDate = new DateTime(1996, 12, 24, 23, 32, 0, DateTimeKind.Utc);

		private static string ApiAuth;

		private static readonly string MANIFEST_NAME = "_manifest-sha1.txt";
		private static readonly string MANIFEST_KEY = Path.GetFileNameWithoutExtension(MANIFEST_NAME);

		static Upload()
		{
			string authFilename = Path.Combine(Globals.RootDirectory, "_api-auth.txt");

			if (File.Exists(authFilename) == false)
				throw new ApplicationException($"API Auth file missing:'{authFilename}'. https://archive.org/account/s3.php Format: 'LOW <Your_S3_access_key>:<Your_S3_secret_key>'");

			ApiAuth = File.ReadAllText(authFilename);
		}

		public static Dictionary<string, string> ParseManifest(string data)
		{
			Dictionary<string, string> fileManifestSHA1s = new Dictionary<string, string>();

			using (StringReader reader = new StringReader(data))
			{
				string line;
				while ((line = reader.ReadLine()) != null)
				{
					string[] parts = line.Split('\t');
					fileManifestSHA1s.Add(parts[0], parts[1]);
				}
			}

			return fileManifestSHA1s;
		}

		public static void MachineRom(string itemName, int batchSize)
		{
			int uploadSleepMs = 500;

			ArchiveOrgItem item = new ArchiveOrgItem(itemName, null, null);
			item.DontCache = true;
			item.GetFile(null);

			dynamic metadata = JsonConvert.DeserializeObject<dynamic>(Tools.Query(item.UrlMetadata));

			DateTime? confilct = null;
			if (metadata.conflict != null)
				confilct = Tools.FromEpochDate((string)metadata.conflict);

			int files_count = 0;
			if (metadata.files_count != null)
				files_count = (int)metadata.files_count;

			bool pending_tasks = false;
			if (metadata.pending_tasks != null)
				pending_tasks = (bool)metadata.pending_tasks;

			bool servers_unavailable = false;
			if (metadata.servers_unavailable != null)
				servers_unavailable = (bool)metadata.servers_unavailable;

			JArray tasks = new JArray();
			if (metadata.tasks != null)
				tasks = metadata.tasks;

			string[] workable_servers = new string[0];
			if (metadata.workable_servers != null)
				workable_servers = ((JArray)metadata.workable_servers).Select(i => i.ToString()).ToArray();

			string host = workable_servers.Length == 0 ? null : workable_servers[workable_servers.Length - 1];

			Tools.ConsoleHeading(3, new string[] {
				"metadata info",
				"=============",
				$"conflict: {(confilct == null ? "OK" : confilct.ToString())}",
				$"files_count: {files_count}",
				$"pending_tasks: {pending_tasks}",
				$"servers_unavailable: {servers_unavailable}",
				$"tasks: {tasks.Count}",
				$"workable_servers: {string.Join(", ", workable_servers)}",
			});

			if (metadata.conflict != null)
				throw new ApplicationException("Archive.org item status is 'conflict' try again later.");

			if (host == null)
				throw new ApplicationException("Archive.org item no workable_servers");

			DataTable machineTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[0], "SELECT machine_id, name, cloneof, description FROM machine ORDER BY machine.name");
			DataTable romTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[0], "SELECT machine_id, sha1, name, merge FROM rom WHERE sha1 IS NOT NULL");

			machineTable.PrimaryKey = new DataColumn[] { machineTable.Columns["name"] };

			DataTable report = Tools.MakeDataTable("Parent Machine ZIP Files",
				"status	action	name	description	sha1",
				"String	String	String	String		String");

			string manifestCacheFilename = Path.Combine(Globals.CacheDirectory, $"upload_{itemName}{MANIFEST_KEY}.txt");

			Dictionary<string, string> fileManifestSHA1s = null;

			ArchiveOrgFile manifestFile = item.GetFile(MANIFEST_KEY);

			if (File.Exists(manifestCacheFilename) == true && (manifestFile == null || Tools.SHA1HexFile(manifestCacheFilename) != manifestFile.sha1))
			{
				Console.WriteLine("!!! Refreshing manifest from cache");
				UploadFile(host, itemName, manifestCacheFilename, MANIFEST_NAME, "", 0);

				fileManifestSHA1s = ParseManifest(File.ReadAllText(manifestCacheFilename, Encoding.ASCII));
			}

			if (fileManifestSHA1s == null)
			{
				if (manifestFile == null)
					fileManifestSHA1s = new Dictionary<string, string>();
				else
					fileManifestSHA1s = ParseManifest(Tools.Query(item.DownloadLink(manifestFile)));
			}

			List<string> parentNames = new List<string>();

			foreach (DataRow parentMachineRow in machineTable.Select("cloneof IS NULL"))
			{
				Dictionary<string, string> nameHashes = GetRomNameHashes(parentMachineRow, machineTable, romTable);

				if (nameHashes.Count == 0)
					continue;

				string name = (string)parentMachineRow["name"];
				string description = (string)parentMachineRow["description"];

				parentNames.Add(name);

				string manifest = CreateRomManifest(nameHashes);
				string manifestSha1 = Tools.SHA1HexText(manifest, Encoding.ASCII);

				DataRow reportRow = report.Rows.Add("", "", name, description, manifestSha1);

				bool noHave = false;
				foreach (string key in nameHashes.Keys)
					if (Globals.RomHashStore.Exists(nameHashes[key]) == false)
					{
						noHave = true;
						break;
					}

				if (noHave == true)
				{
					reportRow["Status"] = "NOHAVE";
					continue;
				}

				if (item.Files.ContainsKey(name) == false)
				{
					reportRow["Status"] = "CREATE";
					continue;
				}

				ArchiveOrgFile file = item.Files[name];

				if (fileManifestSHA1s.ContainsKey(file.sha1) == false)
				{
					reportRow["Status"] = "MISSING";
					continue;
				}

				string remoteManifestSha1 = fileManifestSHA1s[file.sha1];

				if (manifestSha1 != remoteManifestSha1)
				{
					reportRow["Status"] = "REPLACE";
					continue;
				}

				reportRow["Status"] = "OK";
			}

			foreach (string name in item.Files.Keys)
			{
				if (name != MANIFEST_KEY && parentNames.Contains(name) == false)
					report.Rows.Add("DELETE", "", name);
			}

			string[] headings = new string[] { "CREATE", "REPLACE", "DELETE", "MISSING", "OK", "NOHAVE" };

			List<DataView> views = new List<DataView>();
			foreach (string heading in headings)
				views.Add(new DataView(report, $"Status = '{heading}'", null, DataViewRowState.CurrentRows));

			Globals.Reports.SaveHtmlReport(views.ToArray(), headings, report.TableName);

			if (batchSize > 0)
			{
				try
				{
					int count = 0;

					foreach (string action in new string[] { "CREATE", "UPDATE" })
					{
						foreach (DataRow reportRow in report.Select($"Status = '{action}'"))
						{
							string machneName = (string)reportRow["name"];
							DataRow machineRow = machineTable.Rows.Find(machneName);

							Dictionary<string, string> nameHashes = GetRomNameHashes(machineRow, machineTable, romTable);

							using (TempDirectory tempDir = new TempDirectory())
							{
								string targetFilename = Path.Combine(tempDir.Path, machneName + ".zip");
								string tempDirectory = Path.Combine(tempDir.Path, machneName);

								CreateRomArchive(nameHashes, Globals.RomHashStore, tempDirectory, targetFilename);

								string fileSha1 = Tools.SHA1HexFile(targetFilename);

								string manifest = CreateRomManifest(nameHashes);
								string manifestSha1 = Tools.SHA1HexText(manifest, Encoding.ASCII);

								UploadFile(host, itemName, targetFilename, Path.GetFileName(targetFilename), $"{count + 1}/{batchSize} {DateTime.Now}", uploadSleepMs);

								if (fileManifestSHA1s.ContainsKey(fileSha1) == false)
									fileManifestSHA1s.Add(fileSha1, manifestSha1);
								else
									if (fileManifestSHA1s[fileSha1] != manifestSha1)
										throw new ApplicationException($"Manifest SHA1 Missmatch fileSha1:{fileSha1}.");
							}

							reportRow["action"] = "PUT";

							using (StreamWriter writer = new StreamWriter(manifestCacheFilename, false, Encoding.ASCII))
								foreach (string key in fileManifestSHA1s.Keys)
									writer.WriteLine($"{key}\t{fileManifestSHA1s[key]}");

							if (++count >= batchSize)
								break;
						}
					}

					foreach (string action in new string[] { "DELETE", "MISSING" })
					{
						foreach (DataRow reportRow in report.Select($"Status = '{action}'"))
						{
							string machneName = (string)reportRow["name"];
							DeleteFile(itemName, $"{machneName}.zip");

							reportRow["action"] = "DELETE";
						}
					}
				}
				finally
				{
					if (fileManifestSHA1s.Count > 0)
					{
						Console.WriteLine("!!! Uploading manifest");

						UploadFile(host, itemName, manifestCacheFilename, MANIFEST_NAME, "", 0);
					}

					Globals.Reports.SaveHtmlReport(views.ToArray(), headings, report.TableName + " - Actions");
				}
			}
		}


		public static void MachineRomExport(string targetDirectory)
		{
			DataTable machineTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[0], "SELECT machine_id, name, cloneof, description FROM machine ORDER BY machine.name");
			DataTable romTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[0], "SELECT machine_id, sha1, name, merge FROM rom WHERE sha1 IS NOT NULL");

			DataTable report = Tools.MakeDataTable("Machine manifests",
				"name	sha1",
				"String	String");

			string hashLookupFilename = Path.Combine(targetDirectory, MANIFEST_NAME);

			Dictionary<string, string> fileManifestSHA1s = new Dictionary<string, string>();

			using (TempDirectory tempDir = new TempDirectory())
			{
				foreach (DataRow parentMachineRow in machineTable.Select("cloneof IS NULL"))
				{
					string parent_machine_name = (string)parentMachineRow["name"];

					Dictionary<string, string> nameHashes = GetRomNameHashes(parentMachineRow, machineTable, romTable);

					if (nameHashes.Count == 0)
						continue;

					Console.WriteLine(parent_machine_name);

					bool missing = false;
					foreach (string name in nameHashes.Keys)
						if (Globals.RomHashStore.Exists(nameHashes[name]) == false)
						{
							missing = true;
							break;
						}
					if (missing == true)
						continue;

					string manifest = CreateRomManifest(nameHashes);
					string manifestSha1 = Tools.SHA1HexText(manifest, Encoding.ASCII);

					report.Rows.Add(parent_machine_name, manifestSha1);

					string tempDirectory = Path.Combine(tempDir.Path, parent_machine_name);
					string targetFilename = Path.Combine(targetDirectory, parent_machine_name + ".zip");

					CreateRomArchive(nameHashes, Globals.RomHashStore, tempDirectory, targetFilename);

					File.WriteAllText(Path.Combine(targetDirectory, parent_machine_name + ".txt"), manifest, Encoding.ASCII);

					string fileSha1 = Tools.SHA1HexFile(targetFilename);

					string inspectManifest = InspectRomManifest(targetFilename);
					string inspectManifestSha1 = Tools.SHA1HexText(inspectManifest, Encoding.ASCII);

					if (manifestSha1 != inspectManifestSha1)
						throw new ApplicationException($"Bad Minifest Inspection: ${targetFilename}");

					if (fileManifestSHA1s.ContainsKey(fileSha1) == false)
						fileManifestSHA1s.Add(fileSha1, manifestSha1);
					else
						if (fileManifestSHA1s[fileSha1] != manifestSha1)
							throw new ApplicationException($"Manifest SHA1 Missmatch fileSha1:{fileSha1}.");
				}
			}

			using (StreamWriter writer = new StreamWriter(hashLookupFilename, false))
				foreach (string key in fileManifestSHA1s.Keys)
					writer.WriteLine($"{key}\t{fileManifestSHA1s[key]}");

			Globals.Reports.SaveHtmlReport(report, "Machine manifests");
		}

		public static void SoftwareDisk(string itemName, int batchSize, string softwareListName)
		{
			DataTable softwareTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[1],
				"SELECT softwarelist.name AS softwarelist_name, software.name, software.cloneof, software.description, software.software_id " +
				"FROM softwarelist INNER JOIN software ON softwarelist.softwarelist_id = software.softwarelist_id " +
				$"WHERE (softwarelist.name = '{softwareListName}') ORDER BY softwarelist.name, software.name");

			DataTable diskTable = Database.ExecuteFill(Globals.Core.ConnectionStrings[1],
				"SELECT part.software_id, disk.name, disk.sha1 " +
				"FROM (part INNER JOIN diskarea ON part.part_id = diskarea.part_id) INNER JOIN disk ON diskarea.diskarea_id = disk.diskarea_id " +
				"WHERE (disk.sha1 IS NOT NULL)");

			DataTable report = Tools.MakeDataTable("Software Disk Export",
				"name	cloneof	description	rom_name	sha1	in_parent	have",
				"String	String	String		String		String	Boolean		Boolean");

			HashSet<string> hashes = new HashSet<string>();

			for (int pass = 0; pass < 2; ++pass)
			{
				foreach (DataRow softwareRow in softwareTable.Select(pass == 0 ? "cloneof IS NULL" : "cloneof IS NOT NULL"))
				{
					long software_id = (long)softwareRow["software_id"];
					string software_name = (string)softwareRow["name"];
					string cloneof = pass == 0 ? "" : (string)softwareRow["cloneof"];
					string description = (string)softwareRow["description"];

					foreach (DataRow diskRow in diskTable.Select($"software_id = {software_id}"))
					{
						string name = (string)diskRow["name"];
						string sha1 = (string)diskRow["sha1"];

						bool inParent = hashes.Add(sha1) == false;
						bool have = Globals.DiskHashStore.Exists(sha1);

						report.Rows.Add(software_name, cloneof, description, name, sha1, inParent, have);
					}

				}
			}

			Globals.Reports.SaveHtmlReport(report, report.TableName);
		}

		public static void CreateRomArchive(Dictionary<string, string> nameHashes, HashStore hashStore, string tempDirectory, string targetFilename)
		{
			HashSet<string> directories = new HashSet<string>();

			for (int pass = 0; pass < 2; ++pass)
			{
				foreach (string name in nameHashes.Keys)
				{
					List<string> pathParts = new List<string>(name.Split('/'));
					pathParts.Insert(0, tempDirectory);
					string tempFilename = Path.Combine(pathParts.ToArray());

					if (pass == 0)
					{
						string directory = Path.GetDirectoryName(tempFilename);
						if (directories.Add(directory) == true)
							Directory.CreateDirectory(directory);
					}
					else
					{
						File.Copy(hashStore.Filename(nameHashes[name]), tempFilename);
					}
				}
			}

			foreach (string tempFilename in Directory.GetFiles(tempDirectory, "*", SearchOption.AllDirectories))
				File.SetLastWriteTimeUtc(tempFilename, MagicModifiedDate);

			ZipFile.CreateFromDirectory(tempDirectory, targetFilename);
			Directory.Delete(tempDirectory, true);
		}

		public static Dictionary<string, string> GetRomNameHashes(DataRow parentMachineRow, DataTable machineTable, DataTable romTable)
		{
			string parent_machine_name = (string)parentMachineRow["name"];

			Dictionary<string, string> nameHashes = new Dictionary<string, string>();

			GetRomNameHashes(nameHashes, parentMachineRow, romTable);

			foreach (DataRow childMachineRow in machineTable.Select($"cloneof = '{parent_machine_name}'"))
			{
				string child_machine_name = (string)childMachineRow["name"];

				GetRomNameHashes(nameHashes, childMachineRow, romTable);
			}

			return nameHashes;
		}

		private static Dictionary<string, string> GetRomNameHashes(Dictionary<string, string> nameHashes, DataRow machineRow, DataTable romTable)
		{
			long machine_id = (long)machineRow["machine_id"];
			string machine_name = (string)machineRow["name"];
			bool isParent = machineRow.IsNull("cloneof");

			foreach (DataRow romRow in romTable.Select($"machine_id = {machine_id}"))
			{
				if (romRow.IsNull("merge") == false)
					continue;

				string sha1 = (string)romRow["sha1"];
				string name = (string)romRow["name"];

				if (isParent == false)
					name = $"{machine_name}/{name}";

				if (nameHashes.ContainsKey(name) == true)
				{
					if (nameHashes[name] != sha1)
						throw new ApplicationException($"ROM name sha1 mismatch, machine_id:{machine_id}, rom name:{name}.");
					continue;
				}

				nameHashes.Add(name, sha1);
			}

			return nameHashes;
		}

		public static string InspectRomManifest(string zipFilename)
		{
			Dictionary<string, string> nameHashes = new Dictionary<string, string>();

			using (TempDirectory tempDir = new TempDirectory())
			{
				ZipFile.ExtractToDirectory(zipFilename, tempDir.Path);

				Tools.ClearAttributes(tempDir.Path);

				foreach (string filename in Directory.GetFiles(tempDir.Path, "*", SearchOption.AllDirectories))
				{
					string name = filename.Substring(tempDir.Path.Length + 1).Replace('\\', '/');
					string sha1 = Tools.SHA1HexFile(filename);

					nameHashes.Add(name, sha1);
				}
			}

			return CreateRomManifest(nameHashes);
		}

		public static string CreateRomManifest(Dictionary<string, string> nameHashes)
		{
			StringBuilder manifest = new StringBuilder();
			foreach (string name in nameHashes.Keys.OrderBy(i => i))
			{
				manifest.Append(name);
				manifest.Append("\t");
				manifest.Append(nameHashes[name]);
				manifest.Append("\r\n");
			}
			return manifest.ToString();
		}

		public static string UploadFile(string host, string itemName, string sourceFilename, string targetFilename, string info, int sleepMs)
		{
			string url = $"https://{host}/{Uri.EscapeUriString(itemName)}/{Uri.EscapeUriString(targetFilename)}";

			string result;

			FileInfo fileInfo = new FileInfo(sourceFilename);

			lock (Globals.WorkerTaskInfo)
				Globals.WorkerTaskInfo.BytesTotal = fileInfo.Length;

			long total = 0;
			byte[] buffer = new byte[64 * 1024];

			HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
			request.Method = "PUT";
			request.Timeout = 3 * 60 * 60 * 1000;
			
			request.ContentType = "application/octet-stream";
			request.ContentLength = fileInfo.Length;

			request.Headers.Add("authorization", ApiAuth);

			request.Headers.Add("x-archive-size-hint", fileInfo.Length.ToString());
			request.Headers.Add("x-archive-queue-derive", "0");

			request.AllowWriteStreamBuffering = false;

			Console.Write($"Uploading {info} size:{Tools.DataSize(fileInfo.Length)} {url} ...");

			Exception uploadException = null;

			using (FileStream sourceStream = new FileStream(sourceFilename, FileMode.Open))
			{
				try
				{
					using (Stream targetStream = request.GetRequestStream())
					{
						int bytesRead;
						long progress = 0;
						while ((bytesRead = sourceStream.Read(buffer, 0, buffer.Length)) > 0)
						{
							total += bytesRead;
							targetStream.Write(buffer, 0, bytesRead);

							progress += bytesRead;
							if (progress >= Globals.DownloadDotSize)
							{
								Console.Write(".");
								progress = 0;
							}

							lock (Globals.WorkerTaskInfo)
								Globals.WorkerTaskInfo.BytesCurrent = total;

							if (sleepMs > 0)
								Thread.Sleep(sleepMs);
						}
					}
				}
				catch (WebException e)
				{
					uploadException = e;
				}
			}

			using (WebResponse response = request.GetResponse())
			{
				HttpWebResponse httpResponse = (HttpWebResponse)response;

				int statusCode = (int)httpResponse.StatusCode;

				using (Stream responseStream = response.GetResponseStream())
				{
					using (StreamReader reader = new StreamReader(responseStream))
						result = reader.ReadToEnd();
				}

				if (statusCode < 200 || statusCode >= 300)
					throw new ApplicationException($"Upload failed, Bad status code:{statusCode}, url:{url}, response:{result}");

				if (uploadException != null)
					throw new ApplicationException($"Upload failed, status:{statusCode}, url:{url}, message:{uploadException.Message}, response:{result}", uploadException);
			}

			Console.WriteLine("...done.");

			return result;
		}

		public static string DeleteFile(string itemName, string filename)
		{
			string url = $"https://s3.us.archive.org/{Uri.EscapeUriString(itemName)}/{Uri.EscapeUriString(Path.GetFileName(filename))}";

			string result;

			HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
			request.Method = "DELETE";

			request.Headers.Add("authorization", ApiAuth);

			Console.Write($"Delete '{url}' ...");

			using (WebResponse response = request.GetResponse())
			{
				HttpWebResponse httpResponse = (HttpWebResponse)response;

				int statusCode = (int)httpResponse.StatusCode;

				using (Stream responseStream = response.GetResponseStream())
				{
					using (StreamReader reader = new StreamReader(responseStream))
						result = reader.ReadToEnd();
				}

				if (statusCode < 200 || statusCode >= 300)
					throw new ApplicationException($"Bad status code:{statusCode}, url:{url}, response:{result}");
			}
			
			Console.WriteLine("...done.");

			return result;
		}

	}
}
