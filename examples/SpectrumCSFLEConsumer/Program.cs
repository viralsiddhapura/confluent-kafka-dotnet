// Copyright 2024 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Encryption;
using Confluent.SchemaRegistry.Encryption.Aws;
using Confluent.SchemaRegistry.Encryption.Gcp;
using Confluent.SchemaRegistry.Encryption.HcVault;
using Confluent.SchemaRegistry.Serdes;
using Avro.Generic;

namespace Confluent.Kafka.Examples.SpectrumCSFLEConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            string propertiesFile = "kafka-consumer.properties";
            
            if (args.Length > 0)
            {
                propertiesFile = args[0];
            }
            
            Console.WriteLine($"DEBUG: Loading configuration from: {propertiesFile}");
            
            var config = LoadConfiguration(propertiesFile);
            
            // DEBUG: Show loaded configuration (mask sensitive values)
            Console.WriteLine($"DEBUG: Configuration loaded with {config.Count} properties:");
            foreach (var kvp in config)
            {
                string value = kvp.Key.ToLower().Contains("password") || kvp.Key.ToLower().Contains("secret") || kvp.Key.ToLower().Contains("key") 
                    ? MaskSensitiveData(kvp.Value) : kvp.Value;
                Console.WriteLine($"DEBUG:   {kvp.Key} = {value}");
            }
            
            Console.WriteLine($"DEBUG: Registering AWS KMS driver...");
            AwsKmsDriver.Register();
            Console.WriteLine($"DEBUG: AWS KMS driver registered successfully");

            Console.WriteLine($"DEBUG: Creating SchemaRegistryConfig...");
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = config["schema.registry.url"],
                BasicAuthUserInfo = config.ContainsKey("schema.registry.basic.auth.user.info") ? config["schema.registry.basic.auth.user.info"] : null
            };
            Console.WriteLine($"DEBUG: SchemaRegistry URL: {schemaRegistryConfig.Url}");
            Console.WriteLine($"DEBUG: SchemaRegistry Auth configured: {schemaRegistryConfig.BasicAuthUserInfo != null}");

            Console.WriteLine($"DEBUG: Creating ConsumerConfig...");
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = config["bootstrap.servers"],
                GroupId = config["group.id"],
                AutoOffsetReset = Enum.Parse<AutoOffsetReset>(config["auto.offset.reset"], true),
            };
            Console.WriteLine($"DEBUG: Bootstrap servers: {consumerConfig.BootstrapServers}");
            Console.WriteLine($"DEBUG: Group ID: {consumerConfig.GroupId}");
            Console.WriteLine($"DEBUG: Auto offset reset: {consumerConfig.AutoOffsetReset}");
            
            if (config.ContainsKey("security.protocol"))
            {
                consumerConfig.SecurityProtocol = Enum.Parse<SecurityProtocol>(config["security.protocol"], true);
                Console.WriteLine($"DEBUG: Security protocol: {consumerConfig.SecurityProtocol}");
            }
            if (config.ContainsKey("sasl.mechanism"))
            {
                consumerConfig.SaslMechanism = Enum.Parse<SaslMechanism>(config["sasl.mechanism"], true);
                Console.WriteLine($"DEBUG: SASL mechanism: {consumerConfig.SaslMechanism}");
            }
            if (config.ContainsKey("sasl.username"))
            {
                consumerConfig.SaslUsername = config["sasl.username"];
                Console.WriteLine($"DEBUG: SASL username: {consumerConfig.SaslUsername}");
            }
            if (config.ContainsKey("sasl.password"))
            {
                consumerConfig.SaslPassword = config["sasl.password"];
                Console.WriteLine($"DEBUG: SASL password configured: {!string.IsNullOrEmpty(consumerConfig.SaslPassword)}");
            }

            Console.WriteLine($"DEBUG: Creating AvroDeserializerConfig...");
            var avroDeserializerConfig = new AvroDeserializerConfig
            {
                UseLatestVersion = bool.Parse(config["use.latest.version"])
            };
            Console.WriteLine($"DEBUG: UseLatestVersion: {avroDeserializerConfig.UseLatestVersion}");
            
            if (config.ContainsKey("rules.access.key.id"))
            {
                avroDeserializerConfig.Set("rules.access.key.id", config["rules.access.key.id"]);
                Console.WriteLine($"DEBUG: Set AWS access key ID for decryption: {config["rules.access.key.id"]}");
            }
            else
            {
                Console.WriteLine($"DEBUG: WARNING - rules.access.key.id not found in config");
            }
            if (config.ContainsKey("rules.secret.access.key"))
            {
                avroDeserializerConfig.Set("rules.secret.access.key", config["rules.secret.access.key"]);
                Console.WriteLine($"DEBUG: Set AWS secret access key for decryption: {MaskSensitiveData(config["rules.secret.access.key"])}");
            }
            else
            {
                Console.WriteLine($"DEBUG: WARNING - rules.secret.access.key not found in config");
            }
            
            // DEBUG: Show all deserializer config keys
            Console.WriteLine($"DEBUG: AvroDeserializerConfig settings:");
            Console.WriteLine($"DEBUG:   UseLatestVersion: {avroDeserializerConfig.UseLatestVersion}");
            foreach (var kvp in avroDeserializerConfig)
            {
                string value = kvp.Key.ToLower().Contains("secret") || kvp.Key.ToLower().Contains("key") 
                    ? MaskSensitiveData(kvp.Value) : kvp.Value;
                Console.WriteLine($"DEBUG:   {kvp.Key} = {value}");
            }

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; 
                cts.Cancel();
            };

            var consumeTask = Task.Run(() =>
            {
                Console.WriteLine("DEBUG: Starting Spectrum CSFLE Consumer task...");
                try
                {
                    string topicName = config["topic.name"];
                    Console.WriteLine($"DEBUG: Topic name: {topicName}");
                    
                    Console.WriteLine($"DEBUG: Creating CachedSchemaRegistryClient...");
                    using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
                    {
                        Console.WriteLine($"DEBUG: SchemaRegistry client created successfully");
                        
                        // DEBUG: Check what schemas are available  
                        string subjectName = topicName + "-value";
                        Console.WriteLine($"DEBUG: Looking for schema subject: {subjectName}");
                        
                        Console.WriteLine($"DEBUG: Fetching all subjects from Schema Registry...");
                        var allSubjects = await schemaRegistry.GetAllSubjectsAsync();
                        Console.WriteLine($"DEBUG: Available subjects ({allSubjects.Count}): {string.Join(", ", allSubjects)}");
                        
                        Console.WriteLine($"DEBUG: Fetching latest schema for subject: {subjectName}");
                        var latestSchema = await schemaRegistry.GetLatestSchemaAsync(subjectName);
                        Console.WriteLine($"DEBUG: Found schema: ID={latestSchema.Id}, Version={latestSchema.Version}, Type={latestSchema.SchemaType}");
                        Console.WriteLine($"DEBUG: Schema (first 500 chars): {latestSchema.SchemaString.Substring(0, Math.Min(500, latestSchema.SchemaString.Length))}...");
                        
                        // DEBUG: Check for encryption rules
                        Console.WriteLine($"DEBUG: Checking for encryption rules in schema...");
                        if (latestSchema.SchemaString.Contains("confluent:tags"))
                        {
                            Console.WriteLine($"DEBUG: ✓ Schema contains confluent:tags (encryption annotations)");
                        }
                        else
                        {
                            Console.WriteLine($"DEBUG: ⚠ Schema does NOT contain confluent:tags");
                        }
                        if (latestSchema.SchemaString.Contains("PII"))
                        {
                            Console.WriteLine($"DEBUG: ✓ Schema contains PII tags");
                        }
                        else
                        {
                            Console.WriteLine($"DEBUG: ⚠ Schema does NOT contain PII tags");
                        }
                    
                        Console.WriteLine($"DEBUG: Creating AvroDeserializer<GenericRecord>...");
                        var avroDeserializer = new AvroDeserializer<GenericRecord>(schemaRegistry, avroDeserializerConfig);
                        Console.WriteLine($"DEBUG: AvroDeserializer created successfully");
                        
                        Console.WriteLine($"DEBUG: Building Kafka consumer...");
                        using (var consumer =
                        new ConsumerBuilder<string, GenericRecord>(consumerConfig)
                            .SetValueDeserializer(avroDeserializer.AsSyncOverAsync())
                            .SetErrorHandler((_, e) => Console.WriteLine($"DEBUG: Consumer Error Handler triggered: {e.Reason}"))
                            .Build())
                        {
                            Console.WriteLine($"DEBUG: Consumer created successfully, subscribing to topic: {topicName}");
                            consumer.Subscribe(topicName);
                            Console.WriteLine($"DEBUG: Consumer subscribed, waiting for encrypted payroll messages...");
                            Console.WriteLine($"DEBUG: Starting consumption loop...");

                            try
                            {
                                while (!cts.Token.IsCancellationRequested)
                                {
                                    try
                                    {
                                        Console.WriteLine($"DEBUG: Attempting to consume message (timeout: 1s)...");
                                        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(1));
                                        
                                        if (consumeResult == null)
                                        {
                                            Console.WriteLine($"DEBUG: No message received within timeout, continuing...");
                                            continue;
                                        }
                                        
                                        Console.WriteLine($"DEBUG: Raw message received - Partition: {consumeResult.Partition}, Offset: {consumeResult.Offset}, Key: {consumeResult.Message.Key}");
                                        Console.WriteLine($"DEBUG: Message timestamp: {consumeResult.Message.Timestamp}");
                                        Console.WriteLine($"DEBUG: Message headers count: {consumeResult.Message.Headers?.Count ?? 0}");
                                        
                                        Console.WriteLine($"DEBUG: Message consumed from partition {consumeResult.Partition}, offset {consumeResult.Offset}");
                                        
                                        Console.WriteLine($"DEBUG: Attempting to deserialize message value to GenericRecord...");
                                        var genericRecord = consumeResult.Message.Value;
                                        Console.WriteLine($"DEBUG: ✓ Deserialization successful! GenericRecord object created");
                                        
                                        // DEBUG: Check if the generic record looks valid
                                        Console.WriteLine($"DEBUG: GenericRecord null check: {genericRecord == null}");
                                        if (genericRecord != null)
                                        {
                                            Console.WriteLine($"DEBUG: GenericRecord type: {genericRecord.GetType().FullName}");
                                            Console.WriteLine($"DEBUG: GenericRecord schema: {genericRecord.Schema?.Name}");
                                        }
                                        Console.WriteLine($"✓ Successfully decrypted payroll record:");
                                        Console.WriteLine($"  Key: {consumeResult.Message.Key}");
                                        
                                        // Display PII fields (these are encrypted in transit)
                                        Console.WriteLine($"  Amount (PII): {genericRecord["Amount"]}");
                                        Console.WriteLine($"  CheckAmount (PII): {genericRecord["CheckAmount"]}");
                                        
                                        // Display non-PII fields (these are not encrypted)
                                        Console.WriteLine($"  BankAccountNumber: {MaskSensitiveData(genericRecord["BankAccountNumber"]?.ToString())}");
                                        Console.WriteLine($"  GrossEarnings: {genericRecord["GrossEarnings"]}");
                                        Console.WriteLine($"  NetAmount: {genericRecord["NetAmount"]}");
                                        Console.WriteLine($"  SocialSecurityAmount: {genericRecord["SocialSecurityAmount"]}");
                                        Console.WriteLine($"  Hours: {genericRecord["Hours"]}");
                                        
                                        // Show some list data if available
                                        if (genericRecord["DeductionAddonList"] is System.Collections.IList deductionList && deductionList.Count > 0)
                                        {
                                            Console.WriteLine($"  Deductions: {deductionList.Count} items");
                                            foreach (GenericRecord deduction in deductionList)
                                            {
                                                Console.WriteLine($"    - Amount: {deduction["Amount"]}, YTD: {deduction["YtdAmount"]}");
                                            }
                                        }
                                        
                                        if (genericRecord["RegularEarningsList"] is System.Collections.IList earningsList && earningsList.Count > 0)
                                        {
                                            Console.WriteLine($"  Regular Earnings: {earningsList.Count} items");
                                            foreach (GenericRecord earning in earningsList)
                                            {
                                                Console.WriteLine($"    - Amount: {earning["Amount"]}, Hours: {earning["Hours"]}, Rate: {earning["Rate"]}");
                                            }
                                        }
                                        
                                        Console.WriteLine("---");
                                    }
                                    catch (ConsumeException e)
                                    {
                                        Console.WriteLine($"DEBUG: ConsumeException caught!");
                                        Console.WriteLine($"DEBUG: Error Code: {e.Error.Code}");
                                        Console.WriteLine($"DEBUG: Error Reason: {e.Error.Reason}");
                                        Console.WriteLine($"DEBUG: Error IsError: {e.Error.IsError}");
                                        Console.WriteLine($"DEBUG: Error IsFatal: {e.Error.IsFatal}");
                                        
                                        if (e.ConsumerRecord != null)
                                        {
                                            Console.WriteLine($"DEBUG: Failed record - Topic: {e.ConsumerRecord.TopicPartition?.Topic}, Partition: {e.ConsumerRecord.TopicPartition?.Partition}, Offset: {e.ConsumerRecord.Offset}");
                                        }
                                        
                                        if (e.InnerException != null)
                                        {
                                            Console.WriteLine($"DEBUG: Inner exception: {e.InnerException.GetType().Name}: {e.InnerException.Message}");
                                            Console.WriteLine($"DEBUG: Inner exception stack trace: {e.InnerException.StackTrace}");
                                        }
                                        
                                        Console.WriteLine($"Consume ERROR: {e.Error.Reason}");
                                        if (e.Error.Reason.Contains("deserialization"))
                                        {
                                            Console.WriteLine("DEBUG: This appears to be a deserialization/schema mismatch error");
                                            Console.WriteLine("Schema mismatch - skipping message with incompatible schema");
                                            Console.WriteLine($"Partition: {e.ConsumerRecord?.TopicPartition}, Offset: {e.ConsumerRecord?.Offset}");
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"DEBUG: Unexpected exception caught!");
                                        Console.WriteLine($"DEBUG: Exception type: {ex.GetType().FullName}");
                                        Console.WriteLine($"DEBUG: Exception message: {ex.Message}");
                                        if (ex.InnerException != null)
                                        {
                                            Console.WriteLine($"DEBUG: Inner exception: {ex.InnerException.GetType().Name}: {ex.InnerException.Message}");
                                        }
                                        Console.WriteLine($"Unexpected error: {ex.Message}");
                                        Console.WriteLine($"Stack trace: {ex.StackTrace}");
                                    }
                                }
                            }
                            catch (OperationCanceledException)
                            {
                                Console.WriteLine("DEBUG: OperationCanceledException - Consumer cancelled, closing...");
                                consumer.Close();
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"DEBUG: Consumer task exception caught!");
                    Console.WriteLine($"DEBUG: Exception type: {ex.GetType().FullName}");
                    Console.WriteLine($"Consumer task exception: {ex.Message}");
                    if (ex.InnerException != null)
                    {
                        Console.WriteLine($"DEBUG: Inner exception: {ex.InnerException.GetType().Name}: {ex.InnerException.Message}");
                    }
                    Console.WriteLine($"Stack trace: {ex.StackTrace}");
                }
            });

            Console.WriteLine("Spectrum CSFLE Consumer is running. Press Ctrl+C to exit.");
            
            try
            {
                consumeTask.Wait(cts.Token);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Consumer shutdown complete.");
            }
        }
        
        private static Dictionary<string, string> LoadConfiguration(string filePath)
        {
            var config = new Dictionary<string, string>();
            
            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException($"Configuration file not found: {filePath}");
            }
            
            foreach (var line in File.ReadAllLines(filePath))
            {
                var trimmedLine = line.Trim();
                
                // Skip empty lines and comments
                if (string.IsNullOrEmpty(trimmedLine) || trimmedLine.StartsWith("#"))
                    continue;
                
                var equalIndex = trimmedLine.IndexOf('=');
                if (equalIndex > 0)
                {
                    var key = trimmedLine.Substring(0, equalIndex).Trim();
                    var value = trimmedLine.Substring(equalIndex + 1).Trim();
                    config[key] = value;
                }
            }
            
            Console.WriteLine($"DEBUG: Loaded {config.Count} configuration properties from {filePath}");
            return config;
        }
        
        private static string MaskSensitiveData(string data)
        {
            if (string.IsNullOrEmpty(data) || data.Length <= 4)
            {
                return "****";
            }
            return data.Substring(0, 2) + new string('*', data.Length - 4) + data.Substring(data.Length - 2);
        }
    }
}