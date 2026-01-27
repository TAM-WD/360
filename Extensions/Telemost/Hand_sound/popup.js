console.log('🎨 [Popup] Popup открыт');

const extensionEnabledCheckbox = document.getElementById('extensionEnabled');
const handUpEnabledCheckbox = document.getElementById('handUpEnabled');
const handDownEnabledCheckbox = document.getElementById('handDownEnabled');
const mainToggle = document.getElementById('mainToggle');
const toggleStatus = document.getElementById('toggleStatus');
const settingsSection = document.getElementById('settingsSection');
const testUpButton = document.getElementById('testUp');
const testDownButton = document.getElementById('testDown');
const securityIndicator = document.getElementById('securityIndicator');
const securityText = document.getElementById('securityText');

const btnUploadHandUp = document.getElementById('btnUploadHandUp');
const btnUploadHandDown = document.getElementById('btnUploadHandDown');
const uploadHandUp = document.getElementById('uploadHandUp');
const uploadHandDown = document.getElementById('uploadHandDown');
const resetHandUp = document.getElementById('resetHandUp');
const resetHandDown = document.getElementById('resetHandDown');
const handUpStatus = document.getElementById('handUpStatus');
const handDownStatus = document.getElementById('handDownStatus');

chrome.storage.sync.get([
  'extensionEnabled',
  'handUpEnabled',
  'handDownEnabled'
], (result) => {
  console.log('⚙️ [Popup] Загружены настройки:', result);
  
  const isEnabled = result.extensionEnabled !== false;
  extensionEnabledCheckbox.checked = isEnabled;
  updateUIState(isEnabled);
  
  handUpEnabledCheckbox.checked = result.handUpEnabled !== false;
  handDownEnabledCheckbox.checked = result.handDownEnabled !== false;
});

chrome.storage.local.get(['customHandUpSound', 'customHandDownSound'], (result) => {
  if (result.customHandUpSound) {
    handUpStatus.textContent = 'custom';
    handUpStatus.classList.add('custom');
    resetHandUp.disabled = false;
  }
  if (result.customHandDownSound) {
    handDownStatus.textContent = 'custom';
    handDownStatus.classList.add('custom');
    resetHandDown.disabled = false;
  }
});

function updateUIState(enabled) {
  if (enabled) {
    toggleStatus.textContent = 'Включено';
    mainToggle.classList.remove('disabled');
    settingsSection.classList.remove('disabled');
    testUpButton.disabled = false;
    testDownButton.disabled = false;
    securityIndicator.classList.remove('disabled');
    securityText.textContent = 'Прослушивание активно';
  } else {
    toggleStatus.textContent = 'Выключено';
    mainToggle.classList.add('disabled');
    settingsSection.classList.add('disabled');
    testUpButton.disabled = true;
    testDownButton.disabled = true;
    securityIndicator.classList.add('disabled');
    securityText.textContent = 'Прослушивание остановлено';
  }
}

extensionEnabledCheckbox.addEventListener('change', (e) => {
  const enabled = e.target.checked;
  console.log('⚙️ [Popup] extensionEnabled =', enabled);
  
  chrome.storage.sync.set({ extensionEnabled: enabled });
  updateUIState(enabled);
  
  chrome.runtime.sendMessage({
    action: 'extensionToggled',
    enabled: enabled
  });
});

handUpEnabledCheckbox.addEventListener('change', (e) => {
  console.log('⚙️ [Popup] handUpEnabled =', e.target.checked);
  chrome.storage.sync.set({ handUpEnabled: e.target.checked });
});

handDownEnabledCheckbox.addEventListener('change', (e) => {
  console.log('⚙️ [Popup] handDownEnabled =', e.target.checked);
  chrome.storage.sync.set({ handDownEnabled: e.target.checked });
});

function handleFileUpload(fileInput, soundKey, statusElement, resetButton) {
  const file = fileInput.files[0];
  if (!file) return;
  
  if (file.size > 1024 * 1024) {
    alert('Файл слишком большой! Максимальный размер: 1 МБ');
    fileInput.value = '';
    return;
  }
  
  if (!file.type.startsWith('audio/')) {
    alert('Пожалуйста, выберите аудио файл');
    fileInput.value = '';
    return;
  }
  
  console.log('📁 Загрузка файла:', file.name, file.type, file.size);
  
  const reader = new FileReader();
  reader.onload = function(e) {
    const audioData = e.target.result;
    
    chrome.storage.local.set({ [soundKey]: audioData }, () => {
      console.log('✅ Звук сохранен:', soundKey);
      statusElement.textContent = 'custom';
      statusElement.classList.add('custom');
      resetButton.disabled = false;
      
      chrome.runtime.sendMessage({action: 'customSoundUpdated'});
    });
  };
  
  reader.onerror = function() {
    console.error('❌ Ошибка чтения файла');
    alert('Ошибка при загрузке файла');
  };
  
  reader.readAsDataURL(file);
  fileInput.value = '';
}

function resetToDefault(soundKey, statusElement, resetButton) {
  chrome.storage.local.remove(soundKey, () => {
    console.log('🔄 Звук сброшен на default:', soundKey);
    statusElement.textContent = 'default';
    statusElement.classList.remove('custom');
    resetButton.disabled = true;
    
    chrome.runtime.sendMessage({action: 'customSoundUpdated'});
  });
}

btnUploadHandUp.addEventListener('click', () => {
  console.log('📁 Клик на кнопку загрузки (поднятие)');
  uploadHandUp.click();
});

btnUploadHandDown.addEventListener('click', () => {
  console.log('📁 Клик на кнопку загрузки (опускание)');
  uploadHandDown.click();
});

uploadHandUp.addEventListener('change', () => {
  console.log('📄 Выбран файл для поднятия руки');
  handleFileUpload(uploadHandUp, 'customHandUpSound', handUpStatus, resetHandUp);
});

uploadHandDown.addEventListener('change', () => {
  console.log('📄 Выбран файл для опускания руки');
  handleFileUpload(uploadHandDown, 'customHandDownSound', handDownStatus, resetHandDown);
});

resetHandUp.addEventListener('click', () => {
  console.log('🔄 Клик на сброс (поднятие)');
  resetToDefault('customHandUpSound', handUpStatus, resetHandUp);
});

resetHandDown.addEventListener('click', () => {
  console.log('🔄 Клик на сброс (опускание)');
  resetToDefault('customHandDownSound', handDownStatus, resetHandDown);
});

testUpButton.addEventListener('click', () => {
  console.log('🧪 [Popup] Тест поднятия');
  chrome.runtime.sendMessage({ 
    action: 'testSound', 
    sound: 'hand-up.mp3' 
  });
});

testDownButton.addEventListener('click', () => {
  console.log('🧪 [Popup] Тест опускания');
  chrome.runtime.sendMessage({ 
    action: 'testSound', 
    sound: 'hand-down.mp3' 
  });
});

console.log('✅ [Popup] Popup инициализирован');
