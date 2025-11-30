using System.Net;
using Icecake.Ingest.Streaming.Auth;
using Icecake.Ingest.Streaming.Connect;
using Icecake.Ingest.Streaming.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Icecake.Ingest.Streaming.Services;

/// <summary>
/// Provides extension methods for configuring Snowflake-specific services in the dependency injection container.
/// </summary>
public static class SnowpipeServicesExtensions
{
    /// <summary>
    /// Registers and configures services required for integrating with Snowpipe Ingest and adds them to the dependency injection container.
    /// </summary>
    /// <param name="services">The service collection to which the Snowpipe Ingest Client-related services will be added.</param>
    /// <param name="configuration">The application configuration instance used to retrieve settings for Snowpipe client setup.</param>
    /// <param name="sectionRoot">The root section in the configuration that contains Snowflake-related settings. Defaults to "Snowflake".</param>
    /// <returns>The service collection with the added Snowpipe Ingest Client services.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the specified configuration section does not exist.</exception>
    public static IServiceCollection ConfigureRegisterSnowpipeIngestClient(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionRoot = "Snowflake")
    {
        services.AddSnowpipeIngestClient();

        var section = configuration.GetSection(sectionRoot);
        if (!section.Exists())
            throw new InvalidOperationException($"Missing '{sectionRoot}' configuration section.");

        services.AddOptions<AccountOptions>()
            .Bind(section.GetSection("Account"));

        services.AddOptions<CredentialsOptions>()
            .Bind(section.GetSection("Credentials"));

        services.AddOptions<ConnectOptions>()
            .Bind(section.GetSection("Connect"));

        var clientSection = section.GetSection("Client");

        if (clientSection.Exists())
        {
            services.AddOptions<SnowflakeClientOptions>()
                .Bind(clientSection);
        }
        else
        {
            services.AddOptions<SnowflakeClientOptions>()
                .Configure(_ => { /* keep defaults */ });
        }

        return services;
    }

    /// <summary>
    /// Adds and configures services required for the Snowpipe Ingest Client to the dependency injection container.
    /// </summary>
    /// <param name="services">The service collection to which the Snowpipe Ingest Client-related services will be added.</param>
    /// <returns>The service collection with the added Snowpipe Ingest Client services.</returns>
    public static IServiceCollection AddSnowpipeIngestClient(
        this IServiceCollection services)
    {
        // Register options + validation, but no binding here
        services.AddOptions<AccountOptions>()
            .ValidateDataAnnotations()
            .ValidateOnStart();

        services.AddOptions<CredentialsOptions>()
            .ValidateDataAnnotations()
            .ValidateOnStart();

        services.AddOptions<ConnectOptions>()
            .ValidateDataAnnotations()
            .ValidateOnStart();

        services.AddOptions<SnowflakeClientOptions>()
            .ValidateDataAnnotations()
            .ValidateOnStart();

        // Expose static POCO singletons
        services.AddSingleton(sp => sp.GetRequiredService<IOptions<AccountOptions>>().Value);
        services.AddSingleton(sp => sp.GetRequiredService<IOptions<CredentialsOptions>>().Value);
        services.AddSingleton(sp => sp.GetRequiredService<IOptions<ConnectOptions>>().Value);
        services.AddSingleton(sp => sp.GetRequiredService<IOptions<SnowflakeClientOptions>>().Value);

        // HttpClient for JwtScopedTokenAuth
        services.AddHttpClient<JwtScopedTokenAuth>((sp, http) =>
            {
                var account = sp.GetRequiredService<AccountOptions>();
                var opts = sp.GetRequiredService<SnowflakeClientOptions>();

                http.BaseAddress = account.BaseUri;
                http.Timeout = opts.Timeout;
                http.DefaultRequestHeaders.UserAgent.ParseAdd(opts.UserAgent);
                http.DefaultRequestHeaders.Accept.Clear();
                http.DefaultRequestHeaders.Accept.Add(
                    new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/json"));
            })
            .ConfigurePrimaryHttpMessageHandler(sp =>
            {
                var opts = sp.GetRequiredService<SnowflakeClientOptions>();
                return new HttpClientHandler
                {
                    Proxy = opts.Proxy,
                    UseProxy = opts.Proxy != null,
                    AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
                    ServerCertificateCustomValidationCallback = (_, _, _, err) =>
                        !opts.ValidateCertificates || err == System.Net.Security.SslPolicyErrors.None
                };
            });

        // RSA key once (static)
        services.AddSingleton(sp =>
        {
            var creds = sp.GetRequiredService<CredentialsOptions>();
            return RsaKeyMaterial.FromPemFile(creds.PrivateKeyPath, creds.PrivateKeyPassphrase);
        });

        // IAuthProvider implemented by typed client JwtScopedTokenAuth
        services.AddSingleton<IAuthProvider, JwtScopedTokenAuth>();
        services.AddSingleton<IIngestHostProvider>(sp => (IIngestHostProvider)sp.GetRequiredService<IAuthProvider>());

        // HttpClient for SnowpipeIngestClient
        services.AddHttpClient<SnowpipeIngestClient>((sp, http) =>
            {
                var account = sp.GetRequiredService<AccountOptions>();
                var opts = sp.GetRequiredService<SnowflakeClientOptions>();

                http.BaseAddress = account.BaseUri;
                http.Timeout = opts.Timeout;
                http.DefaultRequestHeaders.UserAgent.ParseAdd(opts.UserAgent);
                http.DefaultRequestHeaders.Accept.Clear();
                http.DefaultRequestHeaders.Accept.Add(
                    new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/json"));
            })
            .ConfigurePrimaryHttpMessageHandler(sp =>
            {
                var opts = sp.GetRequiredService<SnowflakeClientOptions>();
                return new HttpClientHandler
                {
                    Proxy = opts.Proxy,
                    UseProxy = opts.Proxy is not null,
                    AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
                    ServerCertificateCustomValidationCallback = (_, _, _, errors) =>
                        !opts.ValidateCertificates || errors == System.Net.Security.SslPolicyErrors.None
                };
            });

        return services;
    }
}