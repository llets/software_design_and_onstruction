const button = document.getElementById("btn")

button.addEventListener("click", function (e) {
  event.preventDefault();
  let reqType =  event.target.getAttribute("action");
  if (reqType === "submit") {
      let type_of_rate = document.getElementById("select-1980").value;
      data = JSON.stringify({ type: type_of_rate});
  }
  console.log('click');
fetch(reqAddr,
{
    method: "POST",
    body: data,
    headers: {
        'Content-Type': 'application/json'
    }
})
.then( response => {
    // fetch в случае успешной отправки возвращает объект Promise,
    // который асинхронно вернет объект response (ответ на запрос)
    response.text().then( function(respdata) {
        // добавляем данные ответа в текстовое поле respField
        respField.value += respdata + "\n";
    });
})
.catch( error => {
    // выводим сообщение об ошибке в модальное окно
    alert(error);
    respField.value += error + "\n";
});
});