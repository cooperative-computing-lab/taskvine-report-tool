// toolbox.js
export class ToolboxItem {
    constructor({ label, key, value = '', type = 'text', isButtonOnly = false, defaultInputBoxText = '', options = [], handler = null }) {
      this.label = label;
      this.key = key;
      this.value = value;
      this.type = 'text';
      this.isButtonOnly = isButtonOnly;
      this.defaultInputBoxText = defaultInputBoxText;
      this.options = options; // Array of {value: string, label: string, handler: function}
      this.isSelector = options.length > 0;
      this.handler = handler;
    }
  
    getInputBoxContent() {
      return this.value;
    }
  
    render() {
      const container = document.createElement('div');
      container.className = this.isButtonOnly ? 'toolbox-row-full' : 'toolbox-row-split';
  
      if (this.isButtonOnly) {
        const btn = document.createElement('button');
        btn.textContent = this.label;
        btn.className = 'toolbox-button';
        btn.addEventListener('click', () => this.handler(this.key));
        container.appendChild(btn);
      } else if (this.isSelector) {
        const select = document.createElement('select');
        select.className = 'toolbox-input';
        select.id = `input-${this.key}`;
        
        this.options.forEach(option => {
          const optionElement = document.createElement('option');
          optionElement.value = option.value;
          optionElement.textContent = option.label;
          select.appendChild(optionElement);
        });
  
        const btn = document.createElement('button');
        btn.textContent = this.label;
        btn.className = 'toolbox-button';
        btn.addEventListener('click', () => {
          const newValue = select.value;
          this.value = newValue;
          // Find and call the handler for the selected option
          const selectedOption = this.options.find(opt => opt.value === newValue);
          if (selectedOption && selectedOption.handler) {
            selectedOption.handler(newValue);
          }
        });
  
        container.appendChild(select);
        container.appendChild(btn);
      } else {
        const input = document.createElement('input');
        input.type = this.type;
        input.value = this.value;
        input.placeholder = this.defaultInputBoxText;
        input.className = 'toolbox-input';
        input.id = `input-${this.key}`;
  
        const btn = document.createElement('button');
        btn.textContent = this.label;
        btn.className = 'toolbox-button';
        btn.addEventListener('click', () => {
          const newValue = input.value;
          this.value = newValue;
          this.handler(this.key, newValue);
        });
  
        container.appendChild(input);
        container.appendChild(btn);
      }
      return container;
    }
  }
  
  export class Toolbox {
    constructor({ title, items = [] }) {
      this.title = title;
      this.items = items.map(item => new ToolboxItem(item));
      this.setupKeyboardShortcuts();
    }
  
    setupKeyboardShortcuts() {
      document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
          const panel = document.querySelector('.toolbox-panel');
          if (panel && panel.classList.contains('show')) {
            panel.classList.add('hide');
            panel.classList.remove('show');
            // Clear all input values when panel is hidden
            const inputs = panel.querySelectorAll('.toolbox-input');
            inputs.forEach(input => {
              input.value = '';
            });
          }
        }
      });
    }
  
    updateHandler(key, handler) {
      const item = this.items.find(item => item.key === key);
      if (!item) {
        throw new Error(`Item '${key}' does not exist.`);
      }
      item.handler = handler;
      
      // Re-render the panel if it exists
      const existingPanel = document.querySelector('.toolbox-panel');
      if (existingPanel) {
        const newPanel = this.renderPanel();
        existingPanel.replaceWith(newPanel);
      }
    }
  
    renderToggle(panel) {
      const btn = document.createElement('button');
      btn.textContent = this.title;
      btn.className = 'toolbox-toggle-button';
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        
        const rect = btn.getBoundingClientRect();
        const buttonLeft = rect.left;
        const buttonTop = rect.top;
        
        const panelWidth = 220;
        const panelHeight = panel.offsetHeight;
        
        // Position panel to the left of the button
        let left = buttonLeft - panelWidth;
        let top = buttonTop;
        
        // Check if panel would go beyond left edge
        if (left < 10) {
          left = 10;
        }
        
        // Check if panel would go beyond bottom edge
        const bottomEdge = top + panelHeight;
        const viewportHeight = window.innerHeight;
        
        if (bottomEdge > viewportHeight) {
          top = viewportHeight - panelHeight - 10;
        }

        panel.style.left = `${left}px`;
        panel.style.top = `${top}px`;
        
        panel.classList.toggle('show');
        panel.classList.toggle('hide');
      });
      
      document.addEventListener('click', (e) => {
        if (!panel.contains(e.target) && !btn.contains(e.target)) {
          panel.classList.add('hide');
          panel.classList.remove('show');
          const inputs = panel.querySelectorAll('.toolbox-input');
          inputs.forEach(input => {
            input.value = '';
          });
        }
      });
      return btn;
    }
  
    renderPanel() {
      const panel = document.createElement('div');
      panel.className = 'toolbox-panel hide';
  
      const group = document.createElement('div');
      group.className = 'toolbox-group';
  
      this.items.forEach(item => {
        if (item.isSelector) {
          const hasOptionHandlers = item.options.some(opt => opt.handler);
          if (hasOptionHandlers) {
            group.appendChild(item.render());
          }
        } else if (item.handler) {
          group.appendChild(item.render());
        }
      });
  
      panel.appendChild(group);
      return panel;
    }
  
    mount(container) {
      const wrapper = document.createElement('div');
      wrapper.className = 'relative';
  
      const panel = this.renderPanel();
      const toggle = this.renderToggle(panel);
  
      wrapper.appendChild(toggle);
      wrapper.appendChild(panel);
      container.appendChild(wrapper);
  
      wrapper.style.position = 'relative';
    }
  }
  
  const toolbox = new Toolbox({
    title: 'Toolbox',
    items: [
      { 
        label: 'Update X', 
        key: 'updateX', 
        defaultInputBoxText: 'Enter X value',
        handler: (key, val) => console.log(`${key} => ${val}`)
      },
      { 
        label: 'Refresh', 
        key: 'refresh', 
        isButtonOnly: true,
        handler: (key) => console.log(`${key} clicked`)
      },
      { 
        label: 'Extract', 
        key: 'mode', 
        options: [
          { 
            value: 'mode1', 
            label: 'Mode 1',
            handler: (value) => console.log(`Mode 1 handler: ${value}`)
          },
          { 
            value: 'mode2', 
            label: 'Mode 2',
            handler: (value) => console.log(`Mode 2 handler: ${value}`)
          },
          { 
            value: 'mode3', 
            label: 'Mode 3',
            handler: (value) => console.log(`Mode 3 handler: ${value}`)
          }
        ]
      }
    ]
  });
  
