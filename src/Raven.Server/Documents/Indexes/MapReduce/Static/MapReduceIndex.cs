﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Client.Exceptions.Indexes;
using Raven.Server.Documents.Indexes.Configuration;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents.Indexes.MapReduce.Static
{
    public class MapReduceIndex : MapReduceIndexBase<MapReduceIndexDefinition>
    {
        private readonly HashSet<CollectionName> _referencedCollections = new HashSet<CollectionName>(CollectionNameComparer.Instance);

        protected internal readonly StaticIndexBase _compiled;

        private HandleReferences _handleReferences;

        private readonly Dictionary<string, AnonymousObjectToBlittableMapResultsEnumerableWrapper> _enumerationWrappers = new Dictionary<string, AnonymousObjectToBlittableMapResultsEnumerableWrapper>();

        private MapReduceIndex(int indexId, MapReduceIndexDefinition definition, StaticIndexBase compiled)
            : base(indexId, IndexType.MapReduce, definition)
        {
            _compiled = compiled;

            if (_compiled.ReferencedCollections == null)
                return;

            foreach (var collection in _compiled.ReferencedCollections)
            {
                foreach (var referencedCollection in collection.Value)
                    _referencedCollections.Add(referencedCollection);
            }
        }

        public override bool HasBoostedFields => _compiled.HasBoostedFields;

        public override bool IsMultiMap => _compiled.Maps.Count > 1 || _compiled.Maps.Any(x => x.Value.Count > 1);

        public static MapReduceIndex CreateNew(int indexId, IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var instance = CreateIndexInstance(indexId, definition);
            ValidateReduceResultsCollectionName(instance, documentDatabase);

            instance.Initialize(documentDatabase,
                new SingleIndexConfiguration(definition.Configuration, documentDatabase.Configuration),
                documentDatabase.Configuration.PerformanceHints);

            return instance;
        }

        private static void ValidateReduceResultsCollectionName(MapReduceIndex index, DocumentDatabase documentDatabase)
        {
            var outputReduceToCollection = index.Definition.OutputReduceToCollection;
            if (string.IsNullOrWhiteSpace(outputReduceToCollection))
                return;

            if (index.Collections.Contains(Constants.Indexing.AllDocumentsCollection, StringComparer.OrdinalIgnoreCase))
            {
                throw new IndexInvalidException($"Cannot output documents from index ({index.Name}) to the collection name ({outputReduceToCollection}) because the index is mapping all documents and this will result in an infinite loop.");
            }
            if (index.Collections.Contains(outputReduceToCollection, StringComparer.OrdinalIgnoreCase))
            {
                throw new IndexInvalidException($"The collection name ({outputReduceToCollection}) cannot be used as this index ({index.Name}) is mapping this collection and this will result in an infinite loop.");
            }

            var indexes = IndexAndTransformerCompilationCache.IndexCache.Where(pair => string.IsNullOrWhiteSpace(pair.Key.OutputReduceToCollection) == false &&
                                                                                       pair.Key.IndexName != index.Definition.Name).ToList();
            foreach (var pair in indexes)
            {
                var otherIndex = pair.Key;
                if (otherIndex.OutputReduceToCollection.Equals(outputReduceToCollection, StringComparison.OrdinalIgnoreCase))
                    throw new IndexInvalidException($"The collection name ({outputReduceToCollection}) which will be used to output documents results should be unique to only one index but it is already used by another index ({otherIndex.IndexName}).");
            }

            foreach (var pair in indexes)
            {
                var otherIndex = pair.Key;
                string description;
                if (otherIndex.Collections.Contains(outputReduceToCollection, StringComparer.OrdinalIgnoreCase) &&
                    CheckIfThereIsAnIndexWhichWillOutputReduceDocumentsWhichWillBeUsedAsMapOnTheSpecifiedIndex(otherIndex, index.Collections.ToArray() /* todo:*/, indexes, out description))
                {
                    description += Environment.NewLine + $"--> {index.Name}: {string.Join(",", index.Collections)} => *{outputReduceToCollection}*";
                    throw new IndexInvalidException($"The collection name ({outputReduceToCollection}) cannot be used to output documents results as it is consumed by other index that will also output results which will lead to an infinite loop:" + Environment.NewLine + description);
                }
            }
        }

        private static bool CheckIfThereIsAnIndexWhichWillOutputReduceDocumentsWhichWillBeUsedAsMapOnTheSpecifiedIndex(
            IndexAndTransformerCompilationCache.CacheKey otherIndex, string[] indexCollections, 
            List<KeyValuePair<IndexAndTransformerCompilationCache.CacheKey, Lazy<StaticIndexBase>>> indexes, out string errorMessage)
        {
            errorMessage = $"{otherIndex.IndexName}: {string.Join(",", otherIndex.Collections)} => {otherIndex.OutputReduceToCollection}";

            if (string.IsNullOrWhiteSpace(otherIndex.OutputReduceToCollection))
                return false;

            if (indexCollections.Contains(otherIndex.OutputReduceToCollection))
                return true;

            foreach (var index in indexes.Where(pair => pair.Key.Collections.Contains(otherIndex.OutputReduceToCollection, StringComparer.OrdinalIgnoreCase)))
            {
                string a;
                var aa = CheckIfThereIsAnIndexWhichWillOutputReduceDocumentsWhichWillBeUsedAsMapOnTheSpecifiedIndex(index.Key, indexCollections, indexes, out a);
                errorMessage += Environment.NewLine + a;
                if (aa)
                {
                    return true;
                }
            }

            return false;
        }

        public static Index Open(int indexId, StorageEnvironment environment, DocumentDatabase documentDatabase)
        {
            var definition = MapIndexDefinition.Load(environment);
            var instance = CreateIndexInstance(indexId, definition);

            instance.Initialize(environment, documentDatabase,
                new SingleIndexConfiguration(definition.Configuration, documentDatabase.Configuration),
                documentDatabase.Configuration.PerformanceHints);

            return instance;
        }

        public static void Update(Index index, IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var staticMapIndex = (MapReduceIndex)index;
            var staticIndex = staticMapIndex._compiled;

            var staticMapIndexDefinition = new MapReduceIndexDefinition(definition, staticIndex.Maps.Keys.ToHashSet(), staticIndex.OutputFields, 
                staticIndex.GroupByFields, staticIndex.HasDynamicFields);
            staticMapIndex.Update(staticMapIndexDefinition, new SingleIndexConfiguration(definition.Configuration, documentDatabase.Configuration));
        }

        private static MapReduceIndex CreateIndexInstance(int indexId, IndexDefinition definition)
        {
            HashSet<string> collections;
            var staticIndex = IndexAndTransformerCompilationCache.GetIndexInstance(definition, out collections);

            var staticMapIndexDefinition = new MapReduceIndexDefinition(definition, collections, staticIndex.OutputFields, 
                staticIndex.GroupByFields, staticIndex.HasDynamicFields);
            var instance = new MapReduceIndex(indexId, staticMapIndexDefinition, staticIndex);

            return instance;
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            var workers = new List<IIndexingWork>();
            workers.Add(new CleanupDeletedDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration, MapReduceWorkContext));

            if (_referencedCollections.Count > 0)
                workers.Add(_handleReferences = new HandleReferences(this, _compiled.ReferencedCollections, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration));

            workers.Add(new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, MapReduceWorkContext, Configuration));
            workers.Add(new ReduceMapResultsOfStaticIndex(this, _compiled.Reduce, Definition, _indexStorage, DocumentDatabase.Metrics, MapReduceWorkContext));

            return workers.ToArray();
        }

        public override void HandleDelete(DocumentTombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            if (_referencedCollections.Count > 0)
                _handleReferences.HandleDelete(tombstone, collection, writer, indexContext, stats);

            base.HandleDelete(tombstone, collection, writer, indexContext, stats);
        }

        public override IIndexedDocumentsEnumerator GetMapEnumerator(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            return new StaticIndexDocsEnumerator(documents, _compiled.Maps[collection], collection, stats);
        }

        public override int HandleMap(LazyStringValue key, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            AnonymousObjectToBlittableMapResultsEnumerableWrapper wrapper;
            if (_enumerationWrappers.TryGetValue(CurrentIndexingScope.Current.SourceCollection, out wrapper) == false)
            {
                _enumerationWrappers[CurrentIndexingScope.Current.SourceCollection] = wrapper = new AnonymousObjectToBlittableMapResultsEnumerableWrapper(this, indexContext);
            }

            wrapper.InitializeForEnumeration(mapResults, indexContext, stats);

            return PutMapResults(key, wrapper, indexContext, stats);
        }

        protected override bool IsStale(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, long? cutoff = null)
        {
            var isStale = base.IsStale(databaseContext, indexContext, cutoff);
            if (isStale || _referencedCollections.Count == 0)
                return isStale;

            return StaticIndexHelper.IsStale(this, databaseContext, indexContext, cutoff);
        }

        protected override unsafe long CalculateIndexEtag(bool isStale, DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            if (_referencedCollections.Count == 0)
                return base.CalculateIndexEtag(isStale, documentsContext, indexContext);

            var minLength = MinimumSizeForCalculateIndexEtagLength();
            var length = minLength +
                         sizeof(long) * 2 * (Collections.Count * _referencedCollections.Count); // last referenced collection etags and last processed reference collection etags

            var indexEtagBytes = stackalloc byte[length];

            CalculateIndexEtagInternal(indexEtagBytes, isStale, documentsContext, indexContext);

            var writePos = indexEtagBytes + minLength;

            return StaticIndexHelper.CalculateIndexEtag(this, length, indexEtagBytes, writePos, documentsContext, indexContext);
        }

        public override Dictionary<string, HashSet<CollectionName>> GetReferencedCollections()
        {
            return _compiled.ReferencedCollections;
        }

        private class AnonymousObjectToBlittableMapResultsEnumerableWrapper : IEnumerable<MapResult>
        {
            private IEnumerable _items;
            private TransactionOperationContext _indexContext;
            private IndexingStatsScope _stats;
            private IndexingStatsScope _createBlittableResultStats;
            private readonly ReduceKeyProcessor _reduceKeyProcessor;
            private readonly HashSet<string> _groupByFields;
            private readonly bool _isMultiMap;
            private PropertyAccessor _propertyAccessor;

            public AnonymousObjectToBlittableMapResultsEnumerableWrapper(MapReduceIndex index, TransactionOperationContext indexContext)
            {
                _indexContext = indexContext;
                _groupByFields = index.Definition.GroupByFields;
                _isMultiMap = index.IsMultiMap;
                _reduceKeyProcessor = new ReduceKeyProcessor(index.Definition.GroupByFields.Count, index._unmanagedBuffersPool);
            }

            public void InitializeForEnumeration(IEnumerable items, TransactionOperationContext indexContext, IndexingStatsScope stats)
            {
                _items = items;
                _indexContext = indexContext;

                if (_stats == stats)
                    return;

                _stats = stats;
                _createBlittableResultStats = _stats.For(IndexingOperation.Reduce.CreateBlittableJson, start: false);
            }

            public IEnumerator<MapResult> GetEnumerator()
            {
                return new Enumerator(_items.GetEnumerator(), this, _createBlittableResultStats);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }


            private class Enumerator : IEnumerator<MapResult>
            {
                private readonly IEnumerator _enumerator;
                private readonly AnonymousObjectToBlittableMapResultsEnumerableWrapper _parent;
                private readonly IndexingStatsScope _createBlittableResult;
                private readonly HashSet<string> _groupByFields;
                private readonly ReduceKeyProcessor _reduceKeyProcessor;

                public Enumerator(IEnumerator enumerator, AnonymousObjectToBlittableMapResultsEnumerableWrapper parent, IndexingStatsScope createBlittableResult)
                {
                    _enumerator = enumerator;
                    _parent = parent;
                    _createBlittableResult = createBlittableResult;
                    _groupByFields = _parent._groupByFields;
                    _reduceKeyProcessor = _parent._reduceKeyProcessor;
                }

                public bool MoveNext()
                {
                    if (_enumerator.MoveNext() == false)
                        return false;

                    var document = _enumerator.Current;

                    using (_createBlittableResult.Start())
                    {
                        PropertyAccessor accessor;

                        if (_parent._isMultiMap == false)
                            accessor = _parent._propertyAccessor ??
                                       (_parent._propertyAccessor = PropertyAccessor.CreateMapReduceOutputAccessor(document.GetType(), _groupByFields));
                        else
                            accessor = TypeConverter.GetPropertyAccessorForMapReduceOutput(document, _groupByFields);

                        var mapResult = new DynamicJsonValue();

                        _reduceKeyProcessor.Reset();


                        var propertiesInOrder = accessor.PropertiesInOrder;
                        int properties = propertiesInOrder.Count;
                        for (int i = 0; i < properties; i++)
                        {
                            var field = propertiesInOrder[i];

                            var value = field.Value.GetValue(document);
                            var blittableValue = TypeConverter.ToBlittableSupportedType(value);
                            mapResult[field.Key] = blittableValue;

                            if (field.Value.IsGroupByField)
                            {
                                _reduceKeyProcessor.Process(_parent._indexContext.Allocator, blittableValue);
                            }
                        }

                        var reduceHashKey = _reduceKeyProcessor.Hash;

                        Current.Data = _parent._indexContext.ReadObject(mapResult, "map-result");
                        Current.ReduceKeyHash = reduceHashKey;
                    }

                    return true;
                }

                public void Reset()
                {
                    throw new System.NotImplementedException();
                }

                public MapResult Current { get; } = new MapResult();

                object IEnumerator.Current => Current;

                public void Dispose()
                {
                    _reduceKeyProcessor.ReleaseBuffer();
                }
            }
        }
    }
}