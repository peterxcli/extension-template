#pragma once

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

class CdfExtension : public Extension {
public:
	void Load(ExtensionLoader &db) override;
	std::string Name() override;
	std::string Version() const override;
};

void CdfPreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

} // namespace duckdb
