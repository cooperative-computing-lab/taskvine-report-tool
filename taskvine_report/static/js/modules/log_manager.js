export class LogManager {
    constructor() {
        this.selectorId = 'log-selector';
        this.runtimeTemplateListAPI = '/api/runtime-template-list';
        this.serverLockAPI = '/api/lock';
        this.serverUnlockAPI = '/api/unlock';
        this.reloadAPI = '/api/reload-runtime-template';
        
        this.selector = document.getElementById(this.selectorId);
        this.reloadButton = document.getElementById('reload-button');

        this._logChangeCallbacks = [];
        this._currentLogFolder = '--- select log ---';

        this._loadingOption = document.createElement('option');
        this._loadingOption.textContent = 'Loading...';
        this._loadingOption.disabled = true;
        this._loadingOption.value = '';
        
        this._selectLogOption = document.createElement('option');
        this._selectLogOption.textContent = this._currentLogFolder;
        this._selectLogOption.disabled = true;
        this._selectLogOption.value = '';
    }

    _setSelectorValue(value) {
        this.selector.innerHTML = '';

        const option = document.createElement('option');
        option.value = value;
        option.textContent = value;
        this.selector.appendChild(option);
        this.selector.value = value;
    }

    async init() {
        this._setSelectorValue(this._currentLogFolder);    

        this.selector.addEventListener('change', async () => {
            const selectedOption = this.selector.selectedOptions[0];

            if (
                !selectedOption ||
                !selectedOption.value ||
                selectedOption.disabled ||
                selectedOption.value === this._currentLogFolder
            ) {
                this.selector.value = this._currentLogFolder;
                return;
            }
        
            this.selector.disabled = true;
            await this._changeLogFolderTo(selectedOption.value);
            this.selector.disabled = false;
        });  
        
        this.selector.addEventListener('focus', () => {
            const previousValue = this.selector.value;
        
            setTimeout(async () => {
                await this._refreshLogOptions();
        
                this.selector.focus();
                if (previousValue) {
                    this.selector.value = previousValue;
                }
            }, 100);
        });        

        this.reloadButton.addEventListener('click', async () => {
            await this._reloadCurrentLog();
        });
    }

    _showLoadingIndicator() {
        this.selector.innerHTML = '';
        this._loadingOption.value = '';
        this._loadingOption.disabled = true;
        this._loadingOption.selected = true;
        this.selector.appendChild(this._loadingOption);
    }

    _showSelectLogOption() {
        this.selector.innerHTML = '';
        this._selectLogOption.disabled = false;
        this._selectLogOption.selected = true;
        this.selector.appendChild(this._selectLogOption);
    }

    _removeLoadingIndicator() {
        this._loadingOption?.remove();
        this._selectLogOption.disabled = false;
        this._selectLogOption.selected = false;
    }

    registerLogChangeCallback(callback) {
        if (typeof callback === 'function') {
            this._logChangeCallbacks.push(callback);
        }
    }

    async _acquireServerLock() {
        try {
            const response = await fetch(this.serverLockAPI, {
                method: 'POST',
            });
            return response.status === 200;
        } catch (error) {
            return false;
        }
    }

    async _releaseServerLock() {
        try {
            const response = await fetch(this.serverUnlockAPI, {
                method: 'POST',
            });
            return response.status === 200;
        } catch (error) {
            return false;
        }
    }

    async _refreshLogOptions() {
        try {
            /* loading log folders from the server */
            this._showLoadingIndicator();
            const response = await fetch(this.runtimeTemplateListAPI);
            const logFolders = await response.json();
            this._removeLoadingIndicator();

            /* list log folders in the selector */
            this._showSelectLogOption();
            logFolders.forEach(folder => {
                const option = document.createElement('option');
                option.value = folder;
                option.textContent = folder;
                this.selector.appendChild(option);
            });

            /* select the current log folder */
            if (this._currentLogFolder && logFolders.includes(this._currentLogFolder)) {
                this.selector.value = this._currentLogFolder;
            }
        } catch (error) {
            this._showSelectLogOption();
            console.error('Error refreshing log options:', error);
        }
    }

    async _changeLogFolderTo(selectedFolder) {
        if (
            !selectedFolder ||
            selectedFolder === this._currentLogFolder ||
            selectedFolder === '--- select log ---' ||
            selectedFolder === 'Loading...'
        ) {
            this.selector.value = this._currentLogFolder;
            return;
        }        
    
        let lockAcquired = false;
    
        try {
            lockAcquired = await this._acquireServerLock();
            if (!lockAcquired) {
                console.warn('Server busy, please try again later');
                this.selector.value = this._currentLogFolder;
                return;
            }
    
            const response = await fetch(`/api/change-runtime-template?runtime_template=${selectedFolder}`);
            if (!response.ok) {
                throw new Error('Failed to change runtime template');
            }
    
            this._currentLogFolder = selectedFolder;
    
            const promises = this._logChangeCallbacks.map(callback => {
                try {
                    return Promise.resolve(callback(this._currentLogFolder));
                } catch (err) {
                    console.error('Error in log change callback:', err);
                    return Promise.resolve();
                }
            });
    
            await Promise.all(promises);
    
        } catch (error) {
            console.error('Template change error:', error);
            this.selector.value = this._currentLogFolder;
        } finally {
            if (lockAcquired) {
                await this._releaseServerLock();
            }
        }
    }

    async _reloadCurrentLog() {
        if (!this._currentLogFolder || this._currentLogFolder === '--- select log ---') {
            return;
        }

        let lockAcquired = false;
        try {
            lockAcquired = await this._acquireServerLock();
            if (!lockAcquired) {
                console.warn('Server busy, please try again later');
                return;
            }

            this.reloadButton.classList.add('loading');
            this.reloadButton.disabled = true;

            const response = await fetch(`${this.reloadAPI}?runtime_template=${this._currentLogFolder}`);
            if (!response.ok) {
                throw new Error('Failed to reload log');
            }

            const promises = this._logChangeCallbacks.map(callback => {
                try {
                    return Promise.resolve(callback(this._currentLogFolder));
                } catch (err) {
                    console.error('Error in log change callback:', err);
                    return Promise.resolve();
                }
            });

            await Promise.all(promises);

        } catch (error) {
            console.error('Reload error:', error);
        } finally {
            if (lockAcquired) {
                await this._releaseServerLock();
            }
            this.reloadButton.classList.remove('loading');
            this.reloadButton.disabled = false;
        }
    }
}
