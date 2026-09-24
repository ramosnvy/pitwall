using System.Collections;

namespace Pitwall.Contracts;

/// <summary>
/// Registro da configuracao efetiva de uma rodada, para que o CSV diga com
/// que parametros cada linha foi medida.
///
/// A auditoria de 24/09 (docs/AUDITORIA-CONFIG.md) achou um valor que o codigo
/// forcava sem ninguem saber -- o Nagle ligado no Kafka. Com a configuracao
/// gravada por rodada, um desvio assim fica visivel no proprio resultado.
///
/// O formato e "chave=valor;chave=valor", sem virgula, para caber numa coluna
/// de CSV sem aspas.
/// </summary>
public static class RunSettings
{
    /// <summary>Junta pares chave-valor numa coluna, em ordem alfabetica.</summary>
    public static string Format(IEnumerable<KeyValuePair<string, string?>> settings)
    {
        var parts = settings
            .Where(kv => kv.Value is not null)
            .OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => $"{Clean(kv.Key)}={Clean(kv.Value!)}");

        var text = string.Join(';', parts);
        return text.Length == 0 ? "padrao" : text;
    }

    /// <summary>
    /// Variaveis de ambiente que mudam o comportamento do runtime .NET
    /// (DOTNET_* e o prefixo antigo COMPlus_*), como a espera ativa do pool de
    /// threads. "nenhuma" quando nao ha.
    /// </summary>
    public static string DotnetEnvironment()
    {
        var pairs = new List<KeyValuePair<string, string?>>();

        foreach (DictionaryEntry entry in Environment.GetEnvironmentVariables())
        {
            var key = (string)entry.Key;

            if (key.StartsWith("DOTNET_", StringComparison.OrdinalIgnoreCase) ||
                key.StartsWith("COMPlus_", StringComparison.OrdinalIgnoreCase))
            {
                pairs.Add(new(key, entry.Value as string));
            }
        }

        return pairs.Count == 0 ? "nenhuma" : Format(pairs);
    }

    // Virgula, ponto e virgula, sinal de igual e quebra de linha quebrariam o
    // formato da coluna.
    private static string Clean(string text) =>
        text.Replace(',', ' ').Replace(';', ' ').Replace('=', ':').Replace('\n', ' ').Replace('\r', ' ');
}
