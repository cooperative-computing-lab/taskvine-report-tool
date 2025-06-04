export class LogManager {
    constructor() {
        this.selectorId = 'log-selector';
        this.runtimeTemplateListAPI = '/api/runtime-template-list';
        this.serverLockAPI = '/api/lock';
        this.serverUnlockAPI = '/api/unlock';
        this.reloadAPI = '/api/reload-runtime-template';

        this.selector = document.getElementById(this.selectorId);
        this.reloadButton = document.getElementById('reload-button');

        if (!this.selector || !this.reloadButton) {
            throw new Error('Selector or reload button not found in DOM');
        }

        this.DEFAULT_PLACEHOLDER = '--- select log ---';

        this._logChangeCallbacks = [];
        this._currentLogFolder = this.DEFAULT_PLACEHOLDER;
        this._reloading = false;
        this._refreshing = false;
        this._abortController = null;

        this._bound = false;
    }

    _createOption(value, text, { disabled = false, selected = false } = {}) {
        const option = document.createElement('option');
        option.value = value;
        option.textContent = text;
        option.disabled = disabled;
        option.selected = selected;
        return option;
    }

    _setSelectorValue(value, triggerChange = false) {
        const options = Array.from(this.selector.options);
        const match = options.find(opt => opt.value === value);
        if (match) {
            match.selected = true;
        } else {
            this.selector.innerHTML = '';
            this.selector.appendChild(this._createOption(value, value, { selected: true }));
        }
        if (triggerChange) {
            this.selector.dispatchEvent(new Event('change'));
        }
    }

    async init() {
        if (this._bound) {
            throw new Error('LogManager.init() should only be called once.');
        }
        this._bound = true;

        this._setSelectorValue(this._currentLogFolder);

        this.selector.addEventListener('change', async () => {
            const selected = this.selector.selectedOptions[0];
            if (!selected || selected.disabled || selected.value === this._currentLogFolder) {
                this._setSelectorValue(this._currentLogFolder);
                return;
            }
            this.selector.disabled = true;
            await this._changeLogFolderTo(selected.value);
            this.selector.disabled = false;
        });

        let refreshTimer = null;
        this.selector.addEventListener('focus', () => {
            clearTimeout(refreshTimer);
            const previousValue = this.selector.value;
            refreshTimer = setTimeout(() => {
                if (this._refreshing) return;
                this._refreshLogOptions(previousValue);
            }, 200);
        });

        this.reloadButton.addEventListener('click', async () => {
            if (this._reloading) return;
            this._reloading = true;
            try {
                await this._reloadCurrentLog();
            } finally {
                this._reloading = false;
            }
        });
    }

    async _refreshLogOptions(previousValue = this._currentLogFolder) {
        this._refreshing = true;

        if (this._abortController) {
            this._abortController.abort();
        }
        this._abortController = new AbortController();

        try {
            this.selector.innerHTML = '';
            this.selector.appendChild(this._createOption('', 'Loading...', { disabled: true, selected: true }));

            const response = await fetch(this.runtimeTemplateListAPI, { signal: this._abortController.signal });
            if (!response.ok) throw new Error(`Server returned ${response.status}`);
            const logFolders = await response.json();

            this.selector.innerHTML = '';
            this.selector.appendChild(this._createOption(this.DEFAULT_PLACEHOLDER, this.DEFAULT_PLACEHOLDER, {
                selected: true
            }));

            logFolders.forEach(folder => {
                this.selector.appendChild(this._createOption(folder, folder));
            });

            const restore = logFolders.includes(previousValue) ? previousValue : this.DEFAULT_PLACEHOLDER;
            this._setSelectorValue(restore);
            this._currentLogFolder = restore;
        } catch (err) {
            console.error('Error refreshing log options:', err);
            this.selector.innerHTML = '';
            this.selector.appendChild(this._createOption(this.DEFAULT_PLACEHOLDER, this.DEFAULT_PLACEHOLDER, {
                selected: true
            }));
            this._currentLogFolder = this.DEFAULT_PLACEHOLDER;
        } finally {
            this._refreshing = false;
        }
    }

    registerLogChangeCallback(callback) {
        if (typeof callback === 'function') {
            this._logChangeCallbacks.push(callback);
        }
    }

    async _acquireServerLock() {
        try {
            const res = await fetch(this.serverLockAPI, { method: 'POST' });
            return res.status === 200;
        } catch {
            return false;
        }
    }

    async _releaseServerLock() {
        try {
            const res = await fetch(this.serverUnlockAPI, { method: 'POST' });
            return res.status === 200;
        } catch {
            return false;
        }
    }

    async _changeLogFolderTo(folder) {
        if (!folder || folder === this._currentLogFolder || folder === this.DEFAULT_PLACEHOLDER) {
            this._setSelectorValue(this._currentLogFolder);
            return;
        }

        let lockAcquired = false;
        try {
            lockAcquired = await this._acquireServerLock();
            if (!lockAcquired) {
                console.warn('Server busy. Try again later.');
                this._setSelectorValue(this._currentLogFolder);
                return;
            }

            const res = await fetch(`/api/change-runtime-template?runtime_template=${folder}`);
            if (!res.ok) throw new Error(`Failed to change log folder: ${res.status}`);

            this._currentLogFolder = folder;
            for (const cb of this._logChangeCallbacks) {
                try {
                    await Promise.resolve(cb(folder));
                } catch (err) {
                    console.warn('Callback failed:', err);
                }
            }
        } catch (err) {
            console.error('Change log folder error:', err);
            this._setSelectorValue(this._currentLogFolder);
        } finally {
            if (lockAcquired) {
                await this._releaseServerLock();
            }
        }
    }

    async _reloadCurrentLog() {
        if (!this._currentLogFolder || this._currentLogFolder === this.DEFAULT_PLACEHOLDER) return;

        let lockAcquired = false;
        try {
            lockAcquired = await this._acquireServerLock();
            if (!lockAcquired) {
                console.warn('Server busy. Try again later.');
                return;
            }

            this.reloadButton.classList.add('loading');
            this.reloadButton.disabled = true;

            const res = await fetch(`${this.reloadAPI}?runtime_template=${this._currentLogFolder}`);
            if (!res.ok) throw new Error(`Reload failed: ${res.status}`);

            for (const cb of this._logChangeCallbacks) {
                try {
                    await Promise.resolve(cb(this._currentLogFolder));
                } catch (err) {
                    console.warn('Callback failed:', err);
                }
            }
            console.info('Reload successful.');
        } catch (err) {
            console.error('Reload error:', err);
        } finally {
            if (lockAcquired) {
                await this._releaseServerLock();
            }
            this.reloadButton.classList.remove('loading');
            this.reloadButton.disabled = false;
        }
    }
}
