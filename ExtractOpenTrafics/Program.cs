using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

class Program
{
    static async Task Main()
    {
        // Charger la configuration
        var configuration = LoadConfiguration();
        string inputDirectory = configuration["InputDirectory"];
        string fileBPath = configuration["FileBPath"];
        string outputDirectory = configuration["OutputDirectory"];
        string logDirectory = configuration["LogDirectory"];

        // Lire les sous-dossiers à partir de la configuration
        var subDirectories = configuration.GetSection("SubDirectories")
            .GetChildren()
            .ToDictionary(x => x.Key, x => x.Value);

        // Assurez-vous que les répertoires existent
        Directory.CreateDirectory(outputDirectory);
        Directory.CreateDirectory(logDirectory);

        // Préparer le fichier de log
        string logFilePath = PrepareLogFile(logDirectory);

        // Lire les valeurs du fichier B
        HashSet<string> valuesFromFileB = new HashSet<string>(await File.ReadAllLinesAsync(fileBPath).ConfigureAwait(false));

        // Traiter les fichiers dans chaque sous-dossier
        foreach (var subDir in subDirectories.Values)
        {
            string fullSubDirPath = Path.Combine(inputDirectory, subDir);
            if (Directory.Exists(fullSubDirPath))
            {
                string[] gzFiles = Directory.GetFiles(fullSubDirPath, "*.gz");

                // Traiter les fichiers .gz en parallèle
                var tasks = gzFiles.Select(filePath => ProcessFileAsync(filePath, valuesFromFileB, outputDirectory, logFilePath, subDir));
                await Task.WhenAll(tasks).ConfigureAwait(false);

                // Supprimer les fichiers sans correspondance
                await DeleteUnmatchedFilesAsync(Path.Combine(outputDirectory, subDir), logFilePath).ConfigureAwait(false);
            }
            else
            {
                await LogAsync(logFilePath, $"Le répertoire {fullSubDirPath} n'existe pas.").ConfigureAwait(false);
            }
        }

        await LogAsync(logFilePath, "Traitement terminé pour tous les fichiers.").ConfigureAwait(false);
    }

    static IConfiguration LoadConfiguration()
    {
        var configurationBuilder = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

        return configurationBuilder.Build();
    }

    static string PrepareLogFile(string logDirectory)
    {
        string logFileName = $"log_{DateTime.Now:yyyyMMdd_HHmmss}.txt";
        string logFilePath = Path.Combine(logDirectory, logFileName);

        // Créer un fichier de log vierge ou écraser s'il existe
        using (FileStream fs = new FileStream(logFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))
        {
            // Juste pour créer le fichier s'il n'existe pas
        }

        return logFilePath;
    }

    static async Task ProcessFileAsync(string filePath, HashSet<string> valuesFromFileB, string outputDirectory, string logFilePath, string subDir)
    {
        string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(filePath);
        string fileExtension = Path.GetExtension(filePath);

        // Vérifier le suffixe du fichier
        if (fileNameWithoutExtension.EndsWith("_msOriginating.txt") && fileExtension == ".gz")
        {
            // Traiter les fichiers avec suffixe '_msOriginating.txt.gz'
            await ProcessMsOriginatingFileAsync(filePath, outputDirectory, logFilePath, subDir).ConfigureAwait(false);
        }
        else if (fileNameWithoutExtension.EndsWith("_mSOriginatingSMSinMSC.txt") && fileExtension == ".gz")
        {
            // Traiter les fichiers avec suffixe '_mSOriginatingSMSinMSC.txt.gz'
            await ProcessMsOriginatingSmsInMscFileAsync(filePath, valuesFromFileB, outputDirectory, logFilePath, subDir).ConfigureAwait(false);
        }
        else
        {
            // Log le fait que le fichier ne correspond pas aux critères et passe à l'itération suivante
            await LogAsync(logFilePath, $"Le fichier {filePath} ne correspond pas aux suffixes attendus.").ConfigureAwait(false);
        }
    }

    static async Task ProcessMsOriginatingFileAsync(string filePath, string outputDirectory, string logFilePath, string subDir)
    {
        string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(filePath);
        string fullOutputSubDir = Path.Combine(outputDirectory, subDir);
        string txtFilePath = Path.Combine(fullOutputSubDir, fileNameWithoutExtension + ".txt");

        // Assurez-vous que le sous-dossier de sortie existe
        Directory.CreateDirectory(fullOutputSubDir);

        try
        {
            bool isMatchFound = false;

            using (FileStream fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
            using (GZipStream gzipStream = new GZipStream(fileStream, CompressionMode.Decompress))
            using (StreamReader reader = new StreamReader(gzipStream))
            using (StreamWriter writer = new StreamWriter(txtFilePath))
            {
                string line;
                bool isHeaderWritten = false;

                while ((line = await reader.ReadLineAsync().ConfigureAwait(false)) != null)
                {
                    // Séparer les colonnes par '|'
                    string[] columns = line.Split('|');

                    if (!isHeaderWritten)
                    {
                        // Écrire l'en-tête dans le fichier de sortie
                        await writer.WriteLineAsync(line).ConfigureAwait(false);
                        isHeaderWritten = true;
                    }
                    else
                    {
                        // Assurez-vous que l'indice de la colonne est correct
                        int calledPartyNumberIndex = 7; // Index pour la colonne 'calledPartyNumber'
                        int subscriptionTypeIndex = 63; // Index pour la colonne 'subscriptionType'

                        if (columns.Length > calledPartyNumberIndex)
                        {
                            string calledPartyNumber = columns[calledPartyNumberIndex].Trim();

                            // Traiter 'calledPartyNumber'
                            if (calledPartyNumber.Length == 8)
                            {
                                calledPartyNumber = "228" + calledPartyNumber; // Ajouter '228'
                            }
                            else if (calledPartyNumber.StartsWith("00228"))
                            {
                                calledPartyNumber = "228" + calledPartyNumber.Substring(5); // Retirer '00' après '00228'
                            }
                            else if (calledPartyNumber.StartsWith("00"))
                            {
                                calledPartyNumber = calledPartyNumber.Substring(2); // Retirer '00'
                            }

                            // Mettre à jour la colonne dans les données
                            columns[calledPartyNumberIndex] = calledPartyNumber;
                        }

                        if (columns.Length > subscriptionTypeIndex)
                        {
                            string subscriptionType = columns[subscriptionTypeIndex].Trim();

                            // Filtrer sur 'subscriptionType'
                            if (subscriptionType == "02")
                            {
                                // Réassembler la ligne avec les colonnes modifiées
                                string modifiedLine = string.Join('|', columns);
                                await writer.WriteLineAsync(modifiedLine).ConfigureAwait(false);
                                isMatchFound = true;
                            }
                        }
                    }
                }
            }

            if (isMatchFound)
            {
                await LogAsync(logFilePath, $"Traitement terminé pour {filePath}. Les lignes correspondantes ont été écrites dans {txtFilePath}").ConfigureAwait(false);
            }
            else
            {
                // Supprimer le fichier de sortie s'il a été créé mais sans correspondance
                if (File.Exists(txtFilePath))
                {
                    File.Delete(txtFilePath);
                }
                await LogAsync(logFilePath, $"Aucune correspondance trouvée pour {filePath}. Le fichier de sortie {txtFilePath} a été supprimé.").ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            await LogAsync(logFilePath, $"Erreur lors du traitement du fichier {filePath}: {ex.Message}").ConfigureAwait(false);
        }
    }


    static async Task ProcessMsOriginatingSmsInMscFileAsync(string filePath, HashSet<string> valuesFromFileB, string outputDirectory, string logFilePath, string subDir)
    {
        string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(filePath);
        string fullOutputSubDir = Path.Combine(outputDirectory, subDir);
        string txtFilePath = Path.Combine(fullOutputSubDir, fileNameWithoutExtension + ".txt");

        // Assurez-vous que le sous-dossier de sortie existe
        Directory.CreateDirectory(fullOutputSubDir);

        try
        {
            bool isMatchFound = false;

            using (FileStream fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
            using (GZipStream gzipStream = new GZipStream(fileStream, CompressionMode.Decompress))
            using (StreamReader reader = new StreamReader(gzipStream))
            using (StreamWriter writer = new StreamWriter(txtFilePath))
            {
                string line;
                bool isHeaderWritten = false;

                while ((line = await reader.ReadLineAsync().ConfigureAwait(false)) != null)
                {
                    // Séparer les colonnes par '|'
                    string[] columns = line.Split('|');

                    if (!isHeaderWritten)
                    {
                        // Écrire l'en-tête dans le fichier de sortie
                        await writer.WriteLineAsync(line).ConfigureAwait(false);
                        isHeaderWritten = true;
                    }
                    else
                    {
                        // Assurez-vous que l'indice des colonnes est correct
                        int destinationAddressIndex = 31; // Index pour la colonne 'destinationAddress'

                        if (columns.Length > destinationAddressIndex)
                        {
                            string destinationAddress = columns[destinationAddressIndex].Trim();

                            // Traiter 'destinationAddress'
                            if (destinationAddress.Length == 8)
                            {
                                destinationAddress = "228" + destinationAddress; // Ajouter '228'
                            }
                            else if (destinationAddress.StartsWith("00228"))
                            {
                                destinationAddress = "228" + destinationAddress.Substring(5); // Retirer '00' après '00228'
                            }
                            else if (destinationAddress.StartsWith("00"))
                            {
                                destinationAddress = destinationAddress.Substring(2); // Retirer '00'
                            }

                            // Mettre à jour la colonne dans les données
                            columns[destinationAddressIndex] = destinationAddress;

                            // Comparer avec les valeurs du fichier B
                            string typeOfCallingSubscriber = columns[4].Trim(); // Index pour la colonne 'typeOfCallingSubscriber'
                            if (valuesFromFileB.Contains(typeOfCallingSubscriber))
                            {
                                // Réassembler la ligne avec les colonnes modifiées
                                string modifiedLine = string.Join('|', columns);
                                await writer.WriteLineAsync(modifiedLine).ConfigureAwait(false);
                                isMatchFound = true;
                            }
                        }
                    }
                }
            }

            if (isMatchFound)
            {
                await LogAsync(logFilePath, $"Traitement terminé pour {filePath}. Les lignes correspondantes ont été écrites dans {txtFilePath}").ConfigureAwait(false);
            }
            else
            {
                // Supprimer le fichier de sortie s'il a été créé mais sans correspondance
                if (File.Exists(txtFilePath))
                {
                    File.Delete(txtFilePath);
                }
                await LogAsync(logFilePath, $"Aucune correspondance trouvée pour {filePath}. Le fichier de sortie {txtFilePath} a été supprimé.").ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            await LogAsync(logFilePath, $"Erreur lors du traitement du fichier {filePath}: {ex.Message}").ConfigureAwait(false);
        }
    }


    static async Task DeleteUnmatchedFilesAsync(string subDirPath, string logFilePath)
    {
        try
        {
            // Obtenir tous les fichiers de sortie dans le sous-dossier
            string[] outputFiles = Directory.GetFiles(subDirPath, "*_output.txt");

            foreach (string file in outputFiles)
            {
                bool isHeaderOnly = await IsHeaderOnlyAsync(file).ConfigureAwait(false);

                if (isHeaderOnly)
                {
                    File.Delete(file);
                    await LogAsync(logFilePath, $"Fichier contenant uniquement l'en-tête supprimé : {file}").ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex)
        {
            await LogAsync(logFilePath, $"Erreur lors de la suppression des fichiers sans correspondance : {ex.Message}").ConfigureAwait(false);
        }
    }

    static async Task<bool> IsHeaderOnlyAsync(string filePath)
    {
        try
        {
            using (StreamReader reader = new StreamReader(filePath))
            {
                string line = await reader.ReadLineAsync().ConfigureAwait(false);
                if (line == null)
                {
                    return false; // Le fichier est vide
                }

                // Lire la prochaine ligne pour vérifier si le fichier contient plus que l'en-tête
                line = await reader.ReadLineAsync().ConfigureAwait(false);

                // Si il n'y a pas de deuxième ligne, alors le fichier ne contient que l'en-tête
                return line == null;
            }
        }
        catch (Exception ex)
        {
            // Log l'erreur si nécessaire
            await LogAsync(filePath, $"Erreur lors de la vérification du fichier {filePath}: {ex.Message}").ConfigureAwait(false);
            return false;
        }
    }

    static async Task LogAsync(string logFilePath, string message)
    {
        // Assurez-vous que les accès concurrentiels au fichier de log sont correctement gérés
        lock (typeof(Program))
        {
            using (StreamWriter logWriter = new StreamWriter(logFilePath, append: true, System.Text.Encoding.UTF8, bufferSize: 4096))
            {
                logWriter.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}");
            }
        }
    }
}
