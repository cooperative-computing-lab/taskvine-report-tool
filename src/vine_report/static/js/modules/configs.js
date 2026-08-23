/**
 * Runtime chart catalog.
 *
 * Titles, descriptions, visibility, grouping, units, and renderer selection
 * come from the backend MetricSpec registry.  Only genuinely custom renderers
 * are named here; every standard metric shares GenericMetricModule.
 */
import { GenericMetricModule } from './generic_metric.js?v=5.9';
import { SourceMetricModule } from './source_metric.js?v=5.9';
import { TaskExecutionDetailsModule } from './task_execution_details.js?v=5.0';
import { TaskSubgraphsModule } from './task_subgraphs.js?v=5.0';

const customRenderers = {
    task_timeline: TaskExecutionDetailsModule,
    task_subgraphs: TaskSubgraphsModule,
};

export const moduleClasses = {};
export const enabledModuleConfigs = [];

export function installMetricCatalog(metrics = []) {
    enabledModuleConfigs.splice(0, enabledModuleConfigs.length);
    Object.keys(moduleClasses).forEach(key => delete moduleClasses[key]);

    metrics.forEach(metric => {
        if (!metric?.id) return;
        const ModuleClass = metric.mode === 'source'
            ? SourceMetricModule
            : customRenderers[metric.renderer] || GenericMetricModule;
        moduleClasses[metric.id] = ModuleClass;
        if (metric.visible === false) return;
        enabledModuleConfigs.push({
            ...metric,
            api_url: metric.mode === 'source' ? '' : `/api/${metric.id}`,
        });
    });
}
