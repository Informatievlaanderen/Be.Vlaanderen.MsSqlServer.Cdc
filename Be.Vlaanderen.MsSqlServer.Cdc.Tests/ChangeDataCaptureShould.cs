using System.Threading.Tasks;
using Xunit;
using Microsoft.Data.SqlClient;

namespace Be.Vlaanderen.MsSqlServer.Cdc.Tests
{
    public class ChangeDataCaptureShould
    {
        [Fact]
        public async Task EnsureCdcOnTable()
        {
            const string testTable = "TestCdc";
            const string testChangeTrackingSchema = "cdc";
            const string testChangeTrackingTable = "dbo_TestCdc_CT";

            // arrange
            const string connectionString = "Server=.;Database=CdcTest;Trusted_Connection=True;TrustServerCertificate=True";
            var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            ChangeDataCapture.EnableCdcOnTable(connection, false, testTable);
            ChangeDataCapture.EnableCdcOnDatabase(connection, false);

            // act
            ChangeDataCapture.EnableCdcOnTable(connection, true, testTable);

            // assert
            Assert.True(ChangeDataCapture.TableExists(connection, $"{testChangeTrackingSchema}.{testChangeTrackingTable}"));
        }
    }
}
