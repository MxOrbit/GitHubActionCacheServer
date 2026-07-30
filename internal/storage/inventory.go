package storage

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

var ErrIndexedObjectMissing = errors.New("indexed storage object is missing")

type IndexedObjectMissingError struct {
	Index int
}

func (e IndexedObjectMissingError) Error() string {
	return fmt.Sprintf("%s: %d", ErrIndexedObjectMissing, e.Index)
}

func (e IndexedObjectMissingError) Unwrap() error {
	return ErrIndexedObjectMissing
}

type ObjectMetadata struct {
	Name       string
	SizeBytes  int64
	ModifiedAt time.Time
}

type FolderContents struct {
	FolderName       string
	Exists           bool
	Objects          []ObjectMetadata
	ObjectCount      int64
	PhysicalBytes    int64
	NewestModifiedAt time.Time
}

func (f FolderContents) LogicalIndexedSize(partCount int) (int64, error) {
	if partCount < 1 {
		return 0, fmt.Errorf("invalid finalized part count %d", partCount)
	}

	sizes := make(map[int]int64, partCount)
	for _, object := range f.Objects {
		index, err := strconv.Atoi(object.Name)
		if err != nil || index < 0 || strconv.Itoa(index) != object.Name {
			continue
		}
		sizes[index] = object.SizeBytes
	}

	total := int64(0)
	for index := range partCount {
		size, ok := sizes[index]
		if !ok {
			return 0, IndexedObjectMissingError{Index: index}
		}
		var err error
		total, err = addInventoryBytes(total, size)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

type ObjectSummary struct {
	ObjectCount      int64
	PhysicalBytes    int64
	NewestModifiedAt time.Time
}

type PartObject struct {
	Index      int
	SizeBytes  int64
	ModifiedAt time.Time
}

type FolderInventory struct {
	FolderName       string
	Parts            []PartObject
	Blocks           ObjectSummary
	Merged           *ObjectMetadata
	Unknown          ObjectSummary
	ObjectCount      int64
	PhysicalBytes    int64
	NewestModifiedAt time.Time
}

type Inventory struct {
	Folders          []FolderInventory
	LooseObjects     []ObjectMetadata
	TemporaryUploads []ObjectMetadata
	ObjectCount      int64
	PhysicalBytes    int64
	NewestModifiedAt time.Time
}

func (i Inventory) Folder(folderName string) (FolderInventory, bool) {
	index := sort.Search(len(i.Folders), func(index int) bool {
		return i.Folders[index].FolderName >= folderName
	})
	if index == len(i.Folders) || i.Folders[index].FolderName != folderName {
		return FolderInventory{}, false
	}
	return i.Folders[index], true
}

func (f FolderInventory) LogicalPartsSize(partCount int) (int64, error) {
	if partCount < 1 {
		return 0, fmt.Errorf("invalid finalized part count %d", partCount)
	}
	if len(f.Parts) < partCount {
		return 0, fmt.Errorf("expected %d finalized parts, found %d", partCount, len(f.Parts))
	}

	total := int64(0)
	for expectedIndex := range partCount {
		position := sort.Search(len(f.Parts), func(index int) bool {
			return f.Parts[index].Index >= expectedIndex
		})
		if position == len(f.Parts) || f.Parts[position].Index != expectedIndex {
			return 0, IndexedObjectMissingError{Index: expectedIndex}
		}
		var err error
		total, err = addInventoryBytes(total, f.Parts[position].SizeBytes)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

type inventoryBuilder struct {
	folders      map[string]*FolderInventory
	looseObjects []ObjectMetadata
	objectCount  int64
	physical     int64
	newest       time.Time
}

func newInventoryBuilder() *inventoryBuilder {
	return &inventoryBuilder{folders: make(map[string]*FolderInventory)}
}

func (b *inventoryBuilder) ensureFolder(folderName string, modifiedAt time.Time) {
	folder := b.folders[folderName]
	if folder == nil {
		folder = &FolderInventory{FolderName: folderName}
		b.folders[folderName] = folder
	}
	folder.NewestModifiedAt = newestTime(folder.NewestModifiedAt, modifiedAt)
	b.newest = newestTime(b.newest, modifiedAt)
}

func (b *inventoryBuilder) addObject(object ObjectMetadata) error {
	if err := validateObjectMetadata(object); err != nil {
		return err
	}
	var err error
	b.physical, err = addInventoryBytes(b.physical, object.SizeBytes)
	if err != nil {
		return err
	}
	b.objectCount++
	b.newest = newestTime(b.newest, object.ModifiedAt)

	folderName, relativeName, found := strings.Cut(object.Name, "/")
	if !found || folderName == "" {
		b.looseObjects = append(b.looseObjects, object)
		return nil
	}

	folder := b.folders[folderName]
	if folder == nil {
		folder = &FolderInventory{FolderName: folderName}
		b.folders[folderName] = folder
	}
	folder.ObjectCount++
	folder.PhysicalBytes, err = addInventoryBytes(folder.PhysicalBytes, object.SizeBytes)
	if err != nil {
		return err
	}
	folder.NewestModifiedAt = newestTime(folder.NewestModifiedAt, object.ModifiedAt)

	relativeObject := object
	relativeObject.Name = relativeName
	switch {
	case relativeName == "merged":
		folder.Merged = &relativeObject
	case strings.HasPrefix(relativeName, "parts/"):
		partName := strings.TrimPrefix(relativeName, "parts/")
		partIndex, parseErr := strconv.Atoi(partName)
		if parseErr == nil && partIndex >= 0 && strconv.Itoa(partIndex) == partName {
			folder.Parts = append(folder.Parts, PartObject{
				Index:      partIndex,
				SizeBytes:  object.SizeBytes,
				ModifiedAt: object.ModifiedAt,
			})
			break
		}
		if err := addObjectSummary(&folder.Unknown, object); err != nil {
			return err
		}
	case validBlockObjectName(relativeName):
		if err := addObjectSummary(&folder.Blocks, object); err != nil {
			return err
		}
	default:
		if err := addObjectSummary(&folder.Unknown, object); err != nil {
			return err
		}
	}
	return nil
}

func (b *inventoryBuilder) build() Inventory {
	folders := make([]FolderInventory, 0, len(b.folders))
	for _, folder := range b.folders {
		sort.Slice(folder.Parts, func(i, j int) bool {
			return folder.Parts[i].Index < folder.Parts[j].Index
		})
		folders = append(folders, *folder)
	}
	sort.Slice(folders, func(i, j int) bool {
		return folders[i].FolderName < folders[j].FolderName
	})
	sort.Slice(b.looseObjects, func(i, j int) bool {
		return b.looseObjects[i].Name < b.looseObjects[j].Name
	})
	return Inventory{
		Folders:          folders,
		LooseObjects:     b.looseObjects,
		ObjectCount:      b.objectCount,
		PhysicalBytes:    b.physical,
		NewestModifiedAt: b.newest,
	}
}

func newFolderContents(folderName string, objects []ObjectMetadata) (FolderContents, error) {
	contents := FolderContents{FolderName: folderName, Objects: objects}
	for _, object := range objects {
		if err := validateObjectMetadata(object); err != nil {
			return FolderContents{}, err
		}
		var err error
		contents.PhysicalBytes, err = addInventoryBytes(contents.PhysicalBytes, object.SizeBytes)
		if err != nil {
			return FolderContents{}, err
		}
		contents.ObjectCount++
		contents.NewestModifiedAt = newestTime(contents.NewestModifiedAt, object.ModifiedAt)
	}
	sort.Slice(contents.Objects, func(i, j int) bool {
		return contents.Objects[i].Name < contents.Objects[j].Name
	})
	return contents, nil
}

func validateObjectMetadata(object ObjectMetadata) error {
	if object.Name == "" {
		return fmt.Errorf("storage inventory contains an object without a name")
	}
	if object.SizeBytes < 0 {
		return fmt.Errorf("storage inventory object %q has negative size %d", object.Name, object.SizeBytes)
	}
	if object.ModifiedAt.IsZero() {
		return fmt.Errorf("storage inventory object %q has no modification time", object.Name)
	}
	return nil
}

func addObjectSummary(summary *ObjectSummary, object ObjectMetadata) error {
	var err error
	summary.PhysicalBytes, err = addInventoryBytes(summary.PhysicalBytes, object.SizeBytes)
	if err != nil {
		return err
	}
	summary.ObjectCount++
	summary.NewestModifiedAt = newestTime(summary.NewestModifiedAt, object.ModifiedAt)
	return nil
}

func addInventoryBytes(total, size int64) (int64, error) {
	if size < 0 || total > math.MaxInt64-size {
		return 0, fmt.Errorf("storage inventory byte count overflows int64")
	}
	return total + size, nil
}

func newestTime(current, candidate time.Time) time.Time {
	if candidate.After(current) {
		return candidate
	}
	return current
}

func validBlockObjectName(relativeName string) bool {
	blockName, encodedName, found := strings.Cut(relativeName, "/")
	return found && blockName == "blocks" && encodedName != "" && !strings.Contains(encodedName, "/")
}

func PartsFolder(folderName string) string {
	return folderName + "/parts"
}

func MergedObject(folderName string) string {
	return folderName + "/merged"
}
