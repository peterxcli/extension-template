#define DUCKDB_EXTENSION_MAIN

#include "cdf_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

string EnableCdfQuery(ClientContext &context, const FunctionParameters &parameters) {
    auto table_name = parameters.values[0].ToString();
    return "CREATE TABLE __duckdb_cdf_" + table_name + "_log AS SELECT *, 'insert' AS _change_type, 0::BIGINT AS _commit_version, current_timestamp AS _commit_timestamp FROM " + table_name + " LIMIT 0;";
}

string VacuumCdfQuery(ClientContext &context, const FunctionParameters &parameters) {
    auto table_name = parameters.values[0].ToString();
    auto interval_str = parameters.values[1].ToString();
    string q1 = "DELETE FROM __duckdb_cdf_" + table_name + "_log WHERE _commit_timestamp < (CURRENT_TIMESTAMP::TIMESTAMP) - INTERVAL '" + interval_str + "'; ";
    string q2 = "FORCE CHECKPOINT;";
    return q1 + q2;
}

static void LoadInternal(ExtensionLoader &loader) {
    auto enable_cdf_pragma = PragmaFunction::PragmaCall("enable_cdf", EnableCdfQuery, {LogicalType::VARCHAR});
    loader.RegisterFunction(enable_cdf_pragma);

    auto vacuum_cdf_pragma = PragmaFunction::PragmaCall("vacuum_cdf", VacuumCdfQuery, {LogicalType::VARCHAR, LogicalType::VARCHAR});
    loader.RegisterFunction(vacuum_cdf_pragma);

    OptimizerExtension cdf_ext;
    cdf_ext.pre_optimize_function = CdfPreOptimize;

    auto &db = loader.GetDatabaseInstance();
    db.config.optimizer_extensions.push_back(cdf_ext);

    Connection con(db);
    con.Query("CREATE MACRO read_cdf(table_name, startingVersion := NULL, startingTimestamp := NULL) AS TABLE "
              "SELECT * FROM query('SELECT * FROM __duckdb_cdf_' || table_name || '_log') "
              "WHERE (startingVersion IS NULL OR _commit_version >= startingVersion) "
              "  AND (startingTimestamp IS NULL OR _commit_timestamp >= startingTimestamp);");
}

void CdfExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string CdfExtension::Name() {
	return "cdf";
}

std::string CdfExtension::Version() const {
#ifdef EXT_VERSION_CDF
	return EXT_VERSION_CDF;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(cdf, loader) {
	duckdb::LoadInternal(loader);
}
}
