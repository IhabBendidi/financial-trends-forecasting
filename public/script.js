// global variable outside angular

var bigramsList 

var entreprises

var date = ""
var colors = [
    "rgba(136, 78, 160)",
    "rgba(203, 67, 53)",
    "rgba(23, 165, 137)",
    "rgba(212, 172, 13)"
]
datasetsList = []


var app = angular.module('myapp', []);

app.controller('MainCtrl', ['$scope', '$window', function ($scope, $window) {

    $scope.entreprises=[]

    $scope.bigramIndex = -1
    $scope.bigramType = ""


    // $scope.bigramsList = $window.bigramsList

    $scope.bigramElement = null

    $scope.pos_neg = true //show 1000 or pos neg all...

    $scope.removeElement = function (obj) {
        for (let i = 0; i < $window.bigramsList.length; i++) {
            if ($window.bigramsList[i].Topic_Bigram === obj.Topic_Bigram) {
                console.log("i will remove:" + obj.Topic_Bigram)
                $window.bigramsList.splice(i, 1);
                $scope.bigramsList = $window.bigramsList
                //remove from graph if added
                if(myChart)
                {
                    for (let i = 0; i < datasetsList.length; i++) {
                        if (obj.Topic_Bigram === datasetsList[i].label) {
                            for (let j = datasetsList.length - 1; j > i; j--) {
                                datasetsList[j].backgroundColor[0] = datasetsList[j - 1].backgroundColor[0]
                                datasetsList[j].borderColor[0] = datasetsList[j - 1].borderColor[0]
                            }
                            datasetsList.splice(i, 1)
                            break;
                        }
                    }
                    plotAll(datasetsList, obj.dates.names)
                }

            }
        }

    }






    $scope.showArticle = function (article) {
        
        $window.open(article.articles_url)
    }
    $scope.showComment = function (url) {
        console.log(url)
        $window.open(url)
    }
    $scope.model = function (obj) {

        for (let i = 0; i < $window.bigramsList.length; i++) {
            console.log(i)
            if ($window.bigramsList[i].Topic_Bigram === obj.Topic_Bigram) {
                $scope.bigramIndex = i;
                break;
            }
        }
        console.log($scope.bigramIndex)
        $scope.bigramType = obj.updown
        $scope.bigramElement = obj
        console.log("i will show model: " + obj.Topic_Bigram)
        $scope.bigram = obj.Topic_Bigram
        $window.location = "#popup1"
        labels2 = []
        values2 = [] //6 valeur
        for (let i = 2; i < 6; i++) {
            values2.push(obj.dates.values[i])
            labels2.push(obj.dates.names[i])
        }
        for (let i = 0; i < 2; i++) {
            values2.push(obj.prediction.values[i])
            labels2.push(obj.prediction.names[i])
        }


        datasetsList2 = []
        p = {
            label: obj.Topic_Bigram,
            fill: false,
            data: values2,
            backgroundColor: [
                'rgb(68, 114, 196)'
            ],
            borderColor: [
                'rgb(68, 114, 196)'
            ],
            borderWidth: 2
        }

        datasetsList2.push(p)
        var ann = [3];
        var ann_labels = ['today'];
        
        var annotations_array = ann.map(function(value, index) {
            return {
                type: 'line',
                id: 'vline' + index,
                mode: 'vertical',
                scaleID: 'x-axis-0',
                value: value,
                borderColor: 'red',
                borderWidth: 2,
                label: {
                    backgroundColor: 'rgba(0,0,0,0.4)',
                    fontColor: "#fff",
                    enabled: true,
                    position: "top",
                    content: ann_labels[index]
                }
            }
        });
    

        var ctx = document.getElementById('myChart2');

        var myChart2 =

            new Chart(ctx, {
                type: 'line',
                data: {
                    labels: labels2,
                    datasets: datasetsList2
                },
                options: {
                    annotation: {
                        drawTime: 'afterDatasetsDraw',
                        annotations: annotations_array,
                    },
                    scales: {
                        yAxes: [{
                            ticks: {
                                beginAtZero: true,
                                stepSize: 20,
                                maxTicksLimit: 100,
                                suggestedMax: 100

                            }
                        }]
                    }
                }
            });






    }

    $scope.modelNeg = function (index) {
        $scope.bigramIndex = index
        $scope.bigramType = "neg"
        console.log("i will show model: " + $scope.hotTopicsPos[index].Topic_Bigram)
        $scope.bigram = $scope.hotTopicsNeg[index].Topic_Bigram
        $window.location = "#popup1"
    }


    $scope.addWord = function (obj) {

        // if (date === "") {
        //     alert("Please, pick a date and click again!")
        //     //return
        // }
        var removed = false;
        for (let i = 0; i < datasetsList.length; i++) {
            if (obj.Topic_Bigram === datasetsList[i].label) {
                for (let j = datasetsList.length - 1; j > i; j--) {
                    datasetsList[j].backgroundColor[0] = datasetsList[j - 1].backgroundColor[0]
                    datasetsList[j].borderColor[0] = datasetsList[j - 1].borderColor[0]
                }
                datasetsList.splice(i, 1)
                removed = true;

                break;
            }
        }
        //topicsElement
        //topicsElementSelected

        if (!removed) {
            if (datasetsList.length >= 4) {
                console.log("max 4")
                return
            }
            p = {
                label: obj.Topic_Bigram,
                fill: false,
                data: obj.dates.values,
                backgroundColor: [
                    colors[datasetsList.length]
                ],
                borderColor: [
                    colors[datasetsList.length]
                ],
                borderWidth: 1
            }

            datasetsList.push(p)
        }
        for (let i = 0; bigramsList.length; i++) {
            if (bigramsList[i].Topic_Bigram === obj.Topic_Bigram) {
                if (removed) {
                    bigramsList[i].class = "topicsElement"
                }
                else {
                    bigramsList[i].class = "topicsElementSelected"
                }

                break;
            }
        }

        plotAll(datasetsList, obj.dates.names)



        console.log("i will add to graphe: " + obj.Topic_Bigram)
    }


}]);

$(document).ready(function () {


    // --------------------------------

    
    // -------------------------
    const input = document.getElementById('example');
    const datepicker = new TheDatepicker.Datepicker(input);
    datepicker.render();

    $('input[type=checkbox][name=chb1]').change(function () {
        var scope = angular.element(document.getElementById("dashContent")).scope();
        if ($("#chb1").prop('checked')) {
            $("#chb3").prop('checked',true)
            scope.pos_neg = false;
        }
        else {
            scope.pos_neg = true;
            $("#chb3").prop('checked',false)
        }
        scope.$apply();
    });
    $('input[type=checkbox][name=chb3]').change(function () {
        var scope = angular.element(document.getElementById("dashContent")).scope();
        if ($("#chb3").prop('checked')) {
            $("#chb1").prop('checked',true)
            scope.pos_neg = false;
        }
        else {
            scope.pos_neg = true;
            $("#chb1").prop('checked',false)
        }
        scope.$apply();
    });

    $("#godate").click(function () {
        while ($("#example").val().includes(".")) {
            $("#example").val($("#example").val().replace(".", "/"))
        }
        date = $("#example").val()

        date = getMonday(date)
        sendDate()
    });

    $('input[type=checkbox][name=chb2]').change(function () {
        alert("checkbox 2 value: " + $("#chb2").prop('checked'))
    });
});

function getMonday(d) {
    d = d.split("/")[1] + "/" + d.split("/")[0] + "/" + d.split("/")[2] + "/"
    console.log(d)
    d = new Date(d);
    var day = d.getDay(),
        diff = d.getDate() - day + (day == 0 ? -6 : 1); // adjust when day is sunday
    return new Date(d.setDate(diff));
}

function plotAll(datasetsList, labelsList) {
    var ctx = document.getElementById('myChart');
    data= {
        labels: labelsList,
        datasets: datasetsList
    }
    if(myChart)
    {
        myChart.data=data
        
        return
    }
    
    var myChart =
        // new Chart(ctx, {
        //     type: 'line',
        //     data: data,
        //     options: options
        // });

        console.log(labelsList)
    //["14/10/19", "21/10/19", "28/10/19", "04/11/19", "11/11/19", "18/11/19"], "values": [80, 10, 55, 44, 77, 88] }, 
    new Chart(ctx, {
        type: 'line',
        data: data,
        options: {
            scales: {
                yAxes: [{
                    ticks: {
                        beginAtZero: true,
                        stepSize: 20,
                        maxTicksLimit: 100,
                        suggestedMax: 100

                    }
                }]
            }
        }
    });
}


function sendDate() {
    // Form Data
    var fd = new FormData();
    fd.append('date', date);


    //send it to back-end
    $.ajax({
        url: "http://localhost:3000/date",
        processData: false,
        contentType: false,
        type: "POST",
        timeout: 600000, // sets timeout to 3 seconds
        data: fd
    })
        .done(function (data) {
            //alert(data)
            //TODO activate the line bellow and remove defaut value
            //bigramsList = data.bigramsList
            var scope = angular.element(document.getElementById("dashContent")).scope();
            scope.bigramsList = data.bigramsList
            scope.entreprises = data.entreprises
            scope.$apply();
            bigramsList=data.bigramsList
            entreprises=data.entreprises

            plotEnreprises()
        })
        .fail(function () {
            alert("error");
        });

}

function plotEnreprises()
{
    colors2=[]
    for(let i=0;i<entreprises.values.length;i++)
    {
        colors2.push('rgb(57, 102, 144,1)')
    }
    datasetsList3 = []
    p = {
        label: "",
        fill: false,
        data: entreprises.values,
        backgroundColor:colors2,
        borderWidth: 1
    }

    datasetsList3.push(p)

    
    var ctx = document.getElementById('myChart3');

    var myChart3 =

        new Chart(ctx, {
            type: 'horizontalBar',
            data: {
                
                labels: entreprises.names,
                datasets: datasetsList3
            },
            options: {
                legend: {
                    display: false
                },
                scales: {
                    xAxes: [
                        {
                            ticks: {
                                min: 0 // Edit the value according to what you need
                            },
                            gridLines: {
                                display:true
                            }
                        }
                    ],
                    yAxes: [
                        {
                            gridLines: {
                                display:false
                            },
                        stacked: true
                        // ticks: {
                        //     beginAtZero: true,
                        //     stepSize:20,
                        //     maxTicksLimit:100,
                        //     suggestedMax:100

                        // }
                    }]
                }
            }
        });
}