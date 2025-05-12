import { BaseModule } from './base.js';

export class TaskCompletionPercentilesModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);
        this.setBottomScaleType('band');
        this.setLeftScaleType('linear');
    }

    plot() {
        if (!this.data) return;
        this.initSVG();

        const barWidth = this.bottomScale.bandwidth() * 0.8;
        const yFormatter = eval(this.data['y_tick_formatter']);

        this.data['points'].forEach(([percentile, time]) => {
            const y = this.leftScale(time);
            const height = -(this.leftScale(time) - this.leftScale(0));

            this.plotRect(
                this.bottomScale(percentile),
                y,
                barWidth,
                height,
                'steelblue',
                1,
                `Percentile: ${percentile}%<br>Time: ${yFormatter(time)}`
            );
        });
    }
} 