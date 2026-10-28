#include "planning/scan_plan/iceberg_scan_plan_state.hpp"
#include "planning/scan_plan/iceberg_manifest_store.hpp"
#include "planning/scan_plan/iceberg_scan_plan_provider.hpp"

namespace duckdb {

IcebergScanPlanState::IcebergScanPlanState(ClientContext &context_p, shared_ptr<IcebergScanInfo> scan_info_p,
                                           string path_p, const IcebergOptions &options_p)
    : context(context_p),
      fs(FileSystem::GetFileSystem(context)), configuration {std::move(scan_info_p), std::move(path_p), nullptr,
                                                             options_p, true} {
}

const IcebergScanConfiguration &IcebergScanPlanState::Configuration() const {
	return configuration;
}

void IcebergScanPlanState::RequireConfigurable() const {
	if (configuration_frozen) {
		throw InternalException("Cannot change Iceberg scan configuration after planning or view creation has started");
	}
}

void IcebergScanPlanState::SetScanInfo(shared_ptr<IcebergScanInfo> scan_info) {
	RequireConfigurable();
	configuration.scan_info = std::move(scan_info);
}

void IcebergScanPlanState::SetOptions(const IcebergOptions &options) {
	RequireConfigurable();
	configuration.options = options;
}

void IcebergScanPlanState::SetTable(IcebergTableSchemaVersion &table) {
	RequireConfigurable();
	configuration.table = table;
}

//! Unlike the scan identity, these two settings only restrict how the provider gets selected, so they can still be
//! added once the configuration is frozen. A host can legitimately estimate cardinality (which selects the provider)
//! before the post-bind hook adds them, e.g. when a scan is bound separately from the query that ends up using it.
//! Only a server-side plan that was already made without them conflicts: a client-side plan reads the manifests,
//! so it satisfies both, and a provider that does not exist yet will pick them up when it gets created.
void IcebergScanPlanState::RequireRestrictablePlanning() const {
	if (scan_plan_provider && !manifest_store) {
		throw InternalException("Cannot restrict Iceberg scan planning after a server-side scan plan was created");
	}
}

void IcebergScanPlanState::DisableServerSidePlanning() {
	if (!configuration.server_side_planning_enabled) {
		return;
	}
	RequireRestrictablePlanning();
	configuration.server_side_planning_enabled = false;
}

void IcebergScanPlanState::RequireRowIds() {
	if (configuration.row_ids_required) {
		return;
	}
	RequireRestrictablePlanning();
	configuration.row_ids_required = true;
}

void IcebergScanPlanState::FreezeConfiguration() {
	if (!configuration.scan_info) {
		throw InternalException("Cannot start Iceberg scan planning without scan metadata");
	}
	configuration_frozen = true;
}

IcebergScanPlanState::~IcebergScanPlanState() = default;

IcebergScanPlanProvider &IcebergScanPlanState::GetScanPlanProvider(const IcebergTableFilters &filters,
                                                                   const IcebergScanOrder &scan_order) {
	if (!scan_plan_provider) {
		FreezeConfiguration();
		auto &scan_info = *configuration.scan_info;
		IcebergScanPlanContext plan_context {context,
		                                     fs,
		                                     configuration.path,
		                                     configuration.options,
		                                     scan_info.snapshot_info,
		                                     scan_info.metadata,
		                                     scan_info.schema,
		                                     scan_info.transaction_data.get()};
		scan_plan_provider =
		    IcebergScanPlanProvider::Create(*this, std::move(plan_context), configuration.table, filters, scan_order,
		                                    configuration.server_side_planning_enabled);
	}
	return *scan_plan_provider;
}

IcebergManifestStore &IcebergScanPlanState::GetManifestStore(IcebergScanPlanContext context) {
	FreezeConfiguration();
	if (!manifest_store) {
		manifest_store = make_uniq<IcebergManifestStore>(lock, std::move(context));
	}
	return *manifest_store;
}

} // namespace duckdb
