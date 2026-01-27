document.getElementById('exportBtn').addEventListener('click', async () => {
  const button = document.getElementById('exportBtn');
  const status = document.getElementById('status');
  const statsContainer = document.getElementById('statsContainer');
  const warningBox = document.getElementById('warningBox');
  
  button.disabled = true;
  status.style.display = 'block';
  status.className = 'info';
  
  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    
    if (!tab.url || !tab.url.includes('/chats/')) {
      status.className = 'error';
      status.textContent = 'Откройте чат трансляции для выгрузки';
      button.disabled = false;
      return;
    }
    
    warningBox.style.display = 'block';
    status.textContent = 'Прокрутка чата и сбор сообщений...';
    
    const results = await chrome.scripting.executeScript({
      target: { tabId: tab.id },
      function: extractChatData
    });

    const data = results[0].result;

    if (data.error) {
      throw new Error(data.error);
    }

    if (data.pairs.length === 0) {
      status.className = 'error';
      status.textContent = 'Не найдено сообщений для выгрузки.';
      button.disabled = false;
      warningBox.style.display = 'none';
      return;
    }

    statsContainer.style.display = 'block';
    document.getElementById('questionsCount').textContent = data.stats.questions;
    document.getElementById('answersCount').textContent = data.stats.answers;
    
    const totalMessagesElement = document.getElementById('totalMessages');
    if (totalMessagesElement) {
      totalMessagesElement.textContent = data.pairs.length;
    }

    createExcelFile(data);

    status.className = 'success';
    status.textContent = '✓ Экспорт успешно завершен!';
    warningBox.style.display = 'none';
  } catch (error) {
    status.className = 'error';
    status.textContent = `Ошибка: ${error.message}`;
    console.error('Export error:', error);
    warningBox.style.display = 'none';
  } finally {
    button.disabled = false;
  }
});

window.addEventListener('DOMContentLoaded', async () => {
  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    const button = document.getElementById('exportBtn');
    const status = document.getElementById('status');
    const warningBox = document.getElementById('warningBox');
    
    if (!tab.url || !tab.url.includes('/chats/')) {
      button.disabled = true;
      status.style.display = 'block';
      status.className = 'error';
      status.textContent = 'Откройте чат трансляции для выгрузки.';
      warningBox.style.display = 'none';
    } else {
      warningBox.style.display = 'block';
    }
  } catch (error) {
    console.error('URL check error:', error);
  }
});

async function extractChatData() {
  try {
    const scrollContainer = document.querySelector('.ui-InfiniteList[data-scroll-container="true"]');
    const infiniteListContainer = document.querySelector('.ui-InfiniteList-Container');
    const chatContainer = document.querySelector('.yamb-conversation__content');
    
    if (!scrollContainer || !infiniteListContainer || !chatContainer) {
      return { error: 'Чат не найден на странице', pairs: [], stats: { questions: 0, answers: 0 } };
    }

    console.log('🔄 Начинаем прокрутку к началу чата...');

    const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

    const scrollWithWheel = (element, deltaY) => {
      const event = new WheelEvent('wheel', {
        deltaY: deltaY,
        deltaMode: 0,
        bubbles: true,
        cancelable: true
      });
      element.dispatchEvent(event);
    };

    const getTransformY = (element) => {
      const style = window.getComputedStyle(element);
      const transform = style.transform;
      if (transform && transform !== 'none') {
        const matrix = transform.match(/matrix.*\((.+)\)/);
        if (matrix) {
          const values = matrix[1].split(', ');
          return parseFloat(values[5] || 0);
        }
      }
      return 0;
    };

    const getFirstMessageId = () => {
      const firstArticle = chatContainer.querySelector('article.message');
      if (firstArticle) {
        const labelledBy = firstArticle.getAttribute('aria-labelledby');
        return labelledBy ? labelledBy.split(' ')[0] : null;
      }
      return null;
    };

    const getLastMessageId = () => {
      const articles = Array.from(chatContainer.querySelectorAll('article.message'));
      
      for (let i = articles.length - 1; i >= 0; i--) {
        const article = articles[i];
        const isSystemMessage = article.querySelector('.yamb-message-system');
        
        if (!isSystemMessage) {
          const messageTextSpan = article.querySelector('span[data-copyable="true"]');
          if (messageTextSpan) {
            const labelledBy = article.getAttribute('aria-labelledby');
            if (labelledBy) {
              return labelledBy.split(' ')[0];
            }
          }
        }
      }
      
      return null;
    };

    console.log('📜 Прокрутка к самому началу...');
    let previousTransformY = getTransformY(infiniteListContainer);
    let previousFirstMessageId = getFirstMessageId();
    let stableCount = 0;
    let scrollAttempts = 0;

    while (stableCount < 15) {
      scrollWithWheel(scrollContainer, -500);
      await sleep(300);
      
      const currentTransformY = getTransformY(infiniteListContainer);
      const currentFirstMessageId = getFirstMessageId();
      
      const transformStable = Math.abs(currentTransformY - previousTransformY) < 5;
      const messageStable = currentFirstMessageId === previousFirstMessageId;
      
      if (transformStable && messageStable) {
        stableCount++;
        console.log(`📍 Позиция стабильна (${stableCount}/15): Transform: ${currentTransformY}px, Первое сообщение: ${currentFirstMessageId}`);
      } else {
        stableCount = 0;
        if (!messageStable) {
          console.log(`📜 Загружены новые сообщения. Первое: ${currentFirstMessageId}`);
        }
      }
      
      previousTransformY = currentTransformY;
      previousFirstMessageId = currentFirstMessageId;
      scrollAttempts++;
      
      if (scrollAttempts % 50 === 0) {
        console.log(`📊 Выполнено ${scrollAttempts} попыток прокрутки вверх`);
      }
    }

    console.log(`✅ Достигнут верх чата после ${scrollAttempts} попыток`);
    await sleep(2000);

    console.log('📥 Начинаем сбор сообщений...');

    const allMessages = new Map();
    let noNewMessagesCount = 0;
    const maxNoNewMessages = 25;
    let previousLastMessageId = null;
    let sameLastMessageCount = 0;

    const collectVisibleMessages = () => {
      const articles = chatContainer.querySelectorAll('article.message');
      let newCount = 0;
      
      articles.forEach((article) => {
        const labelledBy = article.getAttribute('aria-labelledby');
        if (!labelledBy) return;

        const messageId = labelledBy.split(' ')[0];
        if (allMessages.has(messageId)) return;

        const isSystemMessage = article.querySelector('.yamb-message-system');
        if (isSystemMessage) return;

        const messageTextSpan = article.querySelector('span[data-copyable="true"]');
        if (!messageTextSpan) return;

        const messageText = messageTextSpan.textContent.trim();
        const timeElement = article.querySelector('.yamb-message-info__time');
        const time = timeElement ? (timeElement.getAttribute('aria-label') || timeElement.textContent.trim()) : '';

        const isOwnMessage = article.querySelector('.yamb-message-row_own') !== null;

        let sender = '';
        const senderElement = article.querySelector('.yamb-message-user');
        if (senderElement) {
          sender = senderElement.getAttribute('aria-label') || '';
          if (!sender) {
            const nameElement = senderElement.querySelector('.yamb-message-user__name');
            sender = nameElement ? nameElement.textContent.trim() : '';
          }
        }
        
        if (!sender) {
          sender = null;
        }

        const replySection = article.querySelector('.yamb-message-reply');
        let isAnswer = false;
        let questionText = '';
        let questionAuthor = '';

        if (replySection) {
          isAnswer = true;
          const replyTitle = replySection.querySelector('.yamb-message-reply__title');
          const replyDescription = replySection.querySelector('.yamb-message-reply__description');
          
          questionAuthor = replyTitle ? replyTitle.textContent.trim() : '';
          questionText = replyDescription ? replyDescription.textContent.trim() : '';
        }

        allMessages.set(messageId, {
          id: messageId,
          time: time,
          sender: sender,
          isOwnMessage: isOwnMessage,
          type: isAnswer ? 'Ответ' : 'Вопрос',
          message: messageText,
          questionText: questionText,
          questionAuthor: questionAuthor
        });
        
        newCount++;
      });
      
      return newCount;
    };

    collectVisibleMessages();
    console.log(`📊 Начальных сообщений: ${allMessages.size}`);

    while (noNewMessagesCount < maxNoNewMessages) {
      const currentLastMessageId = getLastMessageId();
      
      if (currentLastMessageId === previousLastMessageId) {
        sameLastMessageCount++;
      } else {
        sameLastMessageCount = 0;
        previousLastMessageId = currentLastMessageId;
      }
      
      let scrollStep;
      if (sameLastMessageCount > 8) {
        scrollStep = 150;
      } else if (sameLastMessageCount > 4) {
        scrollStep = 80;
      } else {
        scrollStep = 50;
      }
      
      scrollWithWheel(scrollContainer, scrollStep);
      
      const waitTime = sameLastMessageCount > 5 ? 500 : 350;
      await sleep(waitTime);
      
      const newMessages = collectVisibleMessages();
      
      if (newMessages === 0 && sameLastMessageCount > 5) {
        noNewMessagesCount++;
        console.log(`⏳ Новых сообщений не найдено (${noNewMessagesCount}/${maxNoNewMessages}), последнее: ${currentLastMessageId}, попыток на одном месте: ${sameLastMessageCount}`);
      } else if (newMessages > 0) {
        noNewMessagesCount = 0;
        sameLastMessageCount = 0;
        console.log(`📊 Собрано сообщений: ${allMessages.size} (+${newMessages})`);
      }
      
      if (sameLastMessageCount > 20) {
        console.log(`✅ Достигнут конец чата: последнее сообщение не меняется ${sameLastMessageCount} раз`);
        break;
      }
    }

    console.log(`✅ Всего собрано уникальных сообщений: ${allMessages.size}`);

    const messagesArray = Array.from(allMessages.values());

    let myRealName = null;
    
    messagesArray.forEach(msg => {
      if (msg.type === 'Ответ' && msg.sender && !msg.isOwnMessage) {
        const originalQuestion = messagesArray.find(m => 
          m.type === 'Вопрос' && 
          m.message.toLowerCase().trim() === msg.questionText.toLowerCase().trim() &&
          m.isOwnMessage
        );
        
        if (originalQuestion && msg.questionAuthor) {
          myRealName = msg.questionAuthor;
          console.log(`👤 Найдено реальное имя пользователя: ${myRealName}`);
        }
      }
    });

    if (!myRealName) {
      messagesArray.forEach(msg => {
        if (msg.type === 'Вопрос' && !msg.sender && msg.isOwnMessage) {
          const answerToMyQuestion = messagesArray.find(m =>
            m.type === 'Ответ' &&
            m.questionText.toLowerCase().trim() === msg.message.toLowerCase().trim() &&
            m.questionAuthor
          );
          
          if (answerToMyQuestion) {
            myRealName = answerToMyQuestion.questionAuthor;
            console.log(`👤 Найдено реальное имя из вопроса: ${myRealName}`);
          }
        }
      });
    }

    console.log(`✅ Реальное имя пользователя: ${myRealName || 'Не найдено'}`);

    const questionAuthors = new Map();
    messagesArray.forEach(msg => {
      if (msg.type === 'Ответ' && msg.questionText && msg.questionAuthor) {
        const key = msg.questionText.toLowerCase().trim();
        if (!questionAuthors.has(key)) {
          questionAuthors.set(key, msg.questionAuthor);
        }
      }
    });

    messagesArray.forEach(msg => {
      if (msg.type === 'Вопрос') {
        if (!msg.sender && msg.isOwnMessage && myRealName) {
          msg.sender = myRealName;
        } else if (!msg.sender) {
          const key = msg.message.toLowerCase().trim();
          if (questionAuthors.has(key)) {
            msg.sender = questionAuthors.get(key);
          } else {
            msg.sender = 'Вы';
          }
        }
      }
      
      if (msg.type === 'Ответ') {
        if (!msg.sender && msg.isOwnMessage && myRealName) {
          msg.sender = myRealName;
        } else if (!msg.sender) {
          msg.sender = 'Вы';
        }
      }
    });

    const answerTextMap = new Map();
    messagesArray.forEach(msg => {
      if (msg.type === 'Ответ') {
        const key = `${msg.message.toLowerCase().trim()}_${msg.sender}`;
        answerTextMap.set(key, msg);
      }
    });

    const pairs = [];
    const questionMap = new Map();
    
    messagesArray.forEach(msg => {
      if (msg.type === 'Вопрос') {
        const key = `${msg.message.toLowerCase().trim()}_${msg.sender}`;
        if (!questionMap.has(key)) {
          const pair = {
            questionTime: msg.time,
            questionAuthor: msg.sender,
            questionText: msg.message,
            answers: []
          };
          pairs.push(pair);
          questionMap.set(key, pair);
        }
      }
    });

    messagesArray.forEach(msg => {
      if (msg.type === 'Ответ') {
        const key = `${msg.questionText.toLowerCase().trim()}_${msg.questionAuthor}`;
        const pair = questionMap.get(key);
        
        if (pair) {
          pair.answers.push({
            answerTime: msg.time,
            answerAuthor: msg.sender,
            answerText: msg.message,
            replyToAnswers: []
          });
        } else {
          const answerKey = `${msg.questionText.toLowerCase().trim()}_${msg.questionAuthor}`;
          const originalAnswer = answerTextMap.get(answerKey);
          
          if (originalAnswer) {
            const originalPair = questionMap.get(`${originalAnswer.questionText.toLowerCase().trim()}_${originalAnswer.questionAuthor}`);
            if (originalPair) {
              const targetAnswer = originalPair.answers.find(a => 
                a.answerText.toLowerCase().trim() === originalAnswer.message.toLowerCase().trim() &&
                a.answerAuthor === originalAnswer.sender
              );
              if (targetAnswer) {
                targetAnswer.replyToAnswers.push({
                  replyTime: msg.time,
                  replyAuthor: msg.sender,
                  replyText: msg.message
                });
              }
            }
          }
        }
      }
    });

    const flatPairs = [];
    let questionNumber = 1;
    
    pairs.forEach(pair => {
      if (pair.answers.length === 0) {
        flatPairs.push({
          index: questionNumber,
          questionTime: pair.questionTime,
          questionAuthor: pair.questionAuthor,
          questionText: pair.questionText,
          answerTime: '',
          answerAuthor: '',
          answerText: '',
          replyTime: '',
          replyAuthor: '',
          replyText: '',
          answersCount: 0
        });
        questionNumber++;
      } else {
        pair.answers.forEach((answer, idx) => {
          if (answer.replyToAnswers.length === 0) {
            flatPairs.push({
              index: questionNumber,
              questionTime: idx === 0 ? pair.questionTime : '',
              questionAuthor: idx === 0 ? pair.questionAuthor : '',
              questionText: idx === 0 ? pair.questionText : '',
              answerTime: answer.answerTime,
              answerAuthor: answer.answerAuthor,
              answerText: answer.answerText,
              replyTime: '',
              replyAuthor: '',
              replyText: '',
              answersCount: pair.answers.length,
              isFirstAnswer: idx === 0
            });
          } else {
            answer.replyToAnswers.forEach((reply, replyIdx) => {
              flatPairs.push({
                index: questionNumber,
                questionTime: idx === 0 && replyIdx === 0 ? pair.questionTime : '',
                questionAuthor: idx === 0 && replyIdx === 0 ? pair.questionAuthor : '',
                questionText: idx === 0 && replyIdx === 0 ? pair.questionText : '',
                answerTime: replyIdx === 0 ? answer.answerTime : '',
                answerAuthor: replyIdx === 0 ? answer.answerAuthor : '',
                answerText: replyIdx === 0 ? answer.answerText : '',
                replyTime: reply.replyTime,
                replyAuthor: reply.replyAuthor,
                replyText: reply.replyText,
                answersCount: pair.answers.length,
                isFirstAnswer: idx === 0 && replyIdx === 0
              });
            });
          }
        });
        questionNumber++;
      }
    });

    const stats = {
      questions: pairs.length,
      answers: messagesArray.filter(m => m.type === 'Ответ').length
    };

    console.log('✅ Обработка завершена!');
    console.log(`📊 Статистика: ${stats.questions} вопросов, ${stats.answers} ответов`);

    return { pairs: flatPairs, stats };
  } catch (error) {
    console.error('❌ Ошибка:', error);
    return { error: error.message, pairs: [], stats: { questions: 0, answers: 0 } };
  }
}

function createExcelFile(data) {
  const worksheetData = [
    ['№', 'Время вопроса', 'Автор вопроса', 'Вопрос', 'Время ответа', 'Автор ответа', 'Ответ', 'Время ответа на ответ', 'Автор ответа на ответ', 'Ответ на ответ']
  ];

  data.pairs.forEach(pair => {
    worksheetData.push([
      pair.index,
      pair.questionTime,
      pair.questionAuthor,
      pair.questionText,
      pair.answerTime,
      pair.answerAuthor,
      pair.answerText,
      pair.replyTime,
      pair.replyAuthor,
      pair.replyText
    ]);
  });

  const wb = XLSX.utils.book_new();
  const ws = XLSX.utils.aoa_to_sheet(worksheetData);

  ws['!cols'] = [
    { wch: 5 },
    { wch: 10 },
    { wch: 20 },
    { wch: 50 },
    { wch: 10 },
    { wch: 20 },
    { wch: 50 },
    { wch: 10 },
    { wch: 20 },
    { wch: 50 }
  ];

  if (!ws['!merges']) ws['!merges'] = [];

  let currentRow = 1;
  
  data.pairs.forEach((pair, index) => {
    currentRow++;
    
    if (pair.answersCount > 1 && pair.isFirstAnswer) {
      const startRow = currentRow - 1;
      const endRow = startRow + pair.answersCount - 1;
      
      ws['!merges'].push(
        { s: { r: startRow, c: 0 }, e: { r: endRow, c: 0 } },
        { s: { r: startRow, c: 1 }, e: { r: endRow, c: 1 } },
        { s: { r: startRow, c: 2 }, e: { r: endRow, c: 2 } },
        { s: { r: startRow, c: 3 }, e: { r: endRow, c: 3 } }
      );
    }
  });

  XLSX.utils.book_append_sheet(wb, ws, 'Чат');

  const now = new Date();
  const dateStr = now.toISOString().slice(0, 10);
  const timeStr = now.toTimeString().slice(0, 5).replace(':', '-');
  const filename = `chat_export_${dateStr}_${timeStr}.xlsx`;

  XLSX.writeFile(wb, filename);
}
